package httpserv

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// HealthPath is served by every binary. The underscore makes it an invalid
// S3 bucket name, so it can never collide with S3 requests on the gateway.
const HealthPath = "/_health"

// Setup is what a service provides after flags are parsed.
type Setup struct {
	// Handler serves every path except HealthPath. It is mounted directly,
	// not through http.ServeMux, because ServeMux redirects paths that are
	// not "clean" (S3 keys such as "a//b" or "x/../y" are legal).
	Handler http.Handler
	// Health is an optional readiness check for HealthPath.
	Health HealthFunc
	// Close is called after the server has shut down.
	Close func() error
}

// Service describes one binary for Main.
type Service struct {
	Name        string
	DefaultAddr string
	Setup       func(ctx context.Context, log *slog.Logger) (Setup, error)
}

// Main parses the common flags (-listen, -probe), configures structured JSON
// logging, and serves until SIGINT/SIGTERM. Service-specific flags must be
// registered on flag.CommandLine before calling Main. It never returns.
func Main(s Service) {
	listen := flag.String("listen", s.DefaultAddr, "address to listen on")
	probe := flag.String("probe", "", "GET this URL, exit 0 if it returns 200 (container health check)")
	flag.Parse()

	if *probe != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := Probe(ctx, *probe); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", s.Name)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ln, err := net.Listen("tcp", *listen)
	if err == nil {
		err = run(ctx, s, ln, log)
	}
	if err != nil {
		log.Error("exiting", "err", err)
		stop()
		os.Exit(1)
	}
}

// Root returns the top-level handler: HealthPath goes to the health check,
// everything else to h (404 if h is nil).
func Root(h http.Handler, health HealthFunc) http.Handler {
	hh := HealthHandler(health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == HealthPath {
			hh.ServeHTTP(w, r)
			return
		}
		if h == nil {
			http.NotFound(w, r)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func run(ctx context.Context, s Service, ln net.Listener, log *slog.Logger) (err error) {
	var setup Setup
	if s.Setup != nil {
		if setup, err = s.Setup(ctx, log); err != nil {
			_ = ln.Close()
			return fmt.Errorf("setup: %w", err)
		}
	}
	if setup.Close != nil {
		defer func() { err = errors.Join(err, setup.Close()) }()
	}
	srv := &http.Server{Handler: Root(setup.Handler, setup.Health), ReadHeaderTimeout: 10 * time.Second}
	return Serve(ctx, srv, ln, 10*time.Second, log)
}

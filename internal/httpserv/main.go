package httpserv

import (
	"context"
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

// Service describes one binary for Main.
type Service struct {
	Name        string
	DefaultAddr string
	// Setup is called after flags are parsed. It registers routes on mux and
	// returns an optional health check.
	Setup func(ctx context.Context, mux *http.ServeMux, log *slog.Logger) (HealthFunc, error)
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

func run(ctx context.Context, s Service, ln net.Listener, log *slog.Logger) error {
	mux := http.NewServeMux()
	var health HealthFunc
	if s.Setup != nil {
		h, err := s.Setup(ctx, mux, log)
		if err != nil {
			return fmt.Errorf("setup: %w", err)
		}
		health = h
	}
	mux.Handle("GET /healthz", HealthHandler(health))

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return Serve(ctx, srv, ln, 10*time.Second, log)
}

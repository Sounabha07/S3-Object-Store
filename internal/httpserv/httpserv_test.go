package httpserv

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthHandler(t *testing.T) {
	cases := []struct {
		name  string
		check HealthFunc
		want  int
	}{
		{"nil check", nil, http.StatusOK},
		{"healthy", func(context.Context) error { return nil }, http.StatusOK},
		{"unhealthy", func(context.Context) error { return errors.New("disk full") }, http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			HealthHandler(c.check).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d", rec.Code, c.want)
			}
		})
	}
}

func TestProbe(t *testing.T) {
	ok := httptest.NewServer(HealthHandler(nil))
	defer ok.Close()
	bad := httptest.NewServer(HealthHandler(func(context.Context) error { return errors.New("x") }))
	defer bad.Close()

	ctx := context.Background()
	if err := Probe(ctx, ok.URL); err != nil {
		t.Fatalf("Probe(healthy) = %v", err)
	}
	if err := Probe(ctx, bad.URL); err == nil {
		t.Fatal("Probe(unhealthy) = nil, want error")
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/healthz", HealthHandler(nil))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, srv, ln, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil))) }()

	if err := Probe(context.Background(), "http://"+ln.Addr().String()+"/healthz"); err != nil {
		t.Fatalf("probe before shutdown: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

func TestRunServesSetupRoutesAndHealth(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	healthy := make(chan struct{})
	svc := Service{
		Name: "test",
		Setup: func(ctx context.Context, mux *http.ServeMux, log *slog.Logger) (HealthFunc, error) {
			mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "hi") })
			return func(context.Context) error {
				select {
				case <-healthy:
					return nil
				default:
					return errors.New("starting")
				}
			}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, svc, ln, slog.New(slog.NewTextHandler(io.Discard, nil))) }()

	base := "http://" + ln.Addr().String()
	if err := Probe(context.Background(), base+"/healthz"); err == nil {
		t.Fatal("healthz succeeded before service reported healthy")
	}
	close(healthy)
	if err := Probe(context.Background(), base+"/healthz"); err != nil {
		t.Fatalf("healthz after healthy: %v", err)
	}
	if err := Probe(context.Background(), base+"/hello"); err != nil {
		t.Fatalf("setup route: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run returned %v", err)
	}
}

func TestRunSetupError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	svc := Service{Setup: func(context.Context, *http.ServeMux, *slog.Logger) (HealthFunc, error) {
		return nil, errors.New("boom")
	}}
	if err := run(context.Background(), svc, ln, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("run with failing setup returned nil")
	}
}

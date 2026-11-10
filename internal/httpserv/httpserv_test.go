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

var discard = slog.New(slog.DiscardHandler)

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
			HealthHandler(c.check).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, HealthPath, nil))
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
	if err := Probe(ctx, "http://127.0.0.1:1/unreachable"); err == nil {
		t.Fatal("Probe(unreachable) = nil, want error")
	}
}

func TestRootRouting(t *testing.T) {
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "app:"+r.URL.Path) })
	cases := []struct {
		handler  http.Handler
		path     string
		wantCode int
		wantBody string
	}{
		{app, HealthPath, http.StatusOK, "ok\n"},
		{app, "/bucket/a//b/../c", http.StatusOK, "app:/bucket/a//b/../c"}, // not cleaned or redirected
		{app, "/healthz", http.StatusOK, "app:/healthz"},                   // a legal bucket name
		{nil, "/anything", http.StatusNotFound, ""},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.URL.Path = c.path
		Root(c.handler, nil).ServeHTTP(rec, req)
		if rec.Code != c.wantCode || (c.wantBody != "" && rec.Body.String() != c.wantBody) {
			t.Errorf("%s: got %d %q, want %d %q", c.path, rec.Code, rec.Body.String(), c.wantCode, c.wantBody)
		}
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: Root(nil, nil), ReadHeaderTimeout: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, srv, ln, 5*time.Second, discard) }()

	if err := Probe(context.Background(), "http://"+ln.Addr().String()+HealthPath); err != nil {
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

func TestRunServesSetupHandlerHealthAndClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	healthy := make(chan struct{})
	closed := make(chan struct{})
	svc := Service{
		Name: "test",
		Setup: func(ctx context.Context, log *slog.Logger) (Setup, error) {
			return Setup{
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "hi") }),
				Health: func(context.Context) error {
					select {
					case <-healthy:
						return nil
					default:
						return errors.New("starting")
					}
				},
				Close: func() error { close(closed); return nil },
			}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, svc, ln, discard) }()

	base := "http://" + ln.Addr().String()
	if err := Probe(context.Background(), base+HealthPath); err == nil {
		t.Fatal("health succeeded before service reported healthy")
	}
	close(healthy)
	if err := Probe(context.Background(), base+HealthPath); err != nil {
		t.Fatalf("health after healthy: %v", err)
	}
	if err := Probe(context.Background(), base+"/hello"); err != nil {
		t.Fatalf("setup handler: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run returned %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("Close was not called on shutdown")
	}
}

func TestRunSetupError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := Service{Setup: func(context.Context, *slog.Logger) (Setup, error) {
		return Setup{}, errors.New("boom")
	}}
	if err := run(context.Background(), svc, ln, discard); err == nil {
		t.Fatal("run with failing setup returned nil")
	}
}

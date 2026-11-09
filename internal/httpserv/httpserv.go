// Package httpserv contains the HTTP server lifecycle shared by all binaries:
// graceful shutdown, a /_health endpoint and a probe used by container
// health checks (the runtime image has no curl).
package httpserv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// HealthFunc reports whether the service is healthy. nil means healthy.
type HealthFunc func(ctx context.Context) error

// HealthHandler serves 200 "ok" when check returns nil, 503 otherwise.
// A nil check is always healthy.
func HealthHandler(check HealthFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if check != nil {
			if err := check(r.Context()); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
}

// Serve runs srv on ln until ctx is cancelled, then shuts down gracefully,
// waiting up to grace for in-flight requests.
func Serve(ctx context.Context, srv *http.Server, ln net.Listener, grace time.Duration, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	log.Info("listening", "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Probe GETs url and returns nil on a 200 response. It is used by the
// "-probe" flag of each binary for Docker health checks.
func Probe(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe %s: status %d", url, resp.StatusCode)
	}
	return nil
}

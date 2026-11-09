package storagenode

import (
	"context"
	"net"
	"time"
)

type netDialer struct{ timeout time.Duration }

func (d *netDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	nd := net.Dialer{Timeout: d.timeout, KeepAlive: 30 * time.Second}
	return nd.DialContext(ctx, network, addr)
}

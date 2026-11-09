// Command gateway is the stateless S3-compatible HTTP front end.
//
// M0: serves /healthz only. The S3 API arrives in M1.
package main

import (
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
)

func main() {
	httpserv.Main(httpserv.Service{Name: "gateway", DefaultAddr: ":9000"})
}

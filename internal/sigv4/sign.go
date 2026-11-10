package sigv4

import (
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Sign adds SigV4 headers (X-Amz-Date, X-Amz-Content-Sha256 and
// Authorization) to r. payloadHash is the hex SHA-256 of the body or
// UnsignedPayload. Every header already on r, plus host, is signed. It is
// used by the load generator and chaos tools, which may not depend on the
// AWS SDK; tests check it against the published AWS examples and against
// the Verifier.
func Sign(r *http.Request, accessKey, secret, region, payloadHash string, t time.Time) {
	t = t.UTC()
	date := t.Format(amzDate)
	scope := t.Format("20060102") + "/" + region + "/" + service + "/" + scopeSuffix
	r.Header.Set("X-Amz-Date", date)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	r.Header.Del("Authorization")

	names := []string{"host"}
	for k := range r.Header {
		lk := strings.ToLower(k)
		if lk != "host" && lk != "user-agent" {
			names = append(names, lk)
		}
	}
	sort.Strings(names)
	if r.Host == "" {
		r.Host = r.URL.Host
	}
	p := signingParams{
		accessKey: accessKey, date: t, scope: scope, signedHeaders: names, payloadHash: payloadHash,
	}
	canonical := canonicalRequest(r, p)
	sig := hex.EncodeToString(hmacSHA256(SigningKey(secret, scope), stringToSign(t, scope, canonical)))
	r.Header.Set("Authorization", algorithm+" Credential="+accessKey+"/"+scope+
		", SignedHeaders="+strings.Join(names, ";")+", Signature="+sig)
}

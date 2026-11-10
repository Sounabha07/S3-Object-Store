package s3api

import (
	"net/netip"
	"strings"
	"unicode/utf8"
)

// maxKeyLength is S3's limit on object key length in bytes.
const maxKeyLength = 1024

// validBucketName implements the S3 general-purpose bucket naming rules.
func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '.' || c == '-':
			if i == 0 || i == len(name)-1 {
				return false
			}
		default:
			return false
		}
	}
	if strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	if _, err := netip.ParseAddr(name); err == nil {
		return false
	}
	for _, p := range []string{"xn--", "sthree-", "amzn-s3-demo-"} {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	for _, s := range []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"} {
		if strings.HasSuffix(name, s) {
			return false
		}
	}
	return true
}

// checkKey validates an object key.
func checkKey(key string) *APIError {
	if len(key) > maxKeyLength {
		return ErrKeyTooLong
	}
	if !utf8.ValidString(key) {
		return ErrInvalidArgument.withMessage("Object key must be valid UTF-8.")
	}
	return nil
}

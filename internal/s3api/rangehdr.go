package s3api

import (
	"strconv"
	"strings"
)

// byteRange is the result of parsing a Range header against an object size.
type byteRange struct {
	start, length int64
	partial       bool // false means "send the whole object with 200"
}

// parseRange interprets a Range header the way S3 does:
//
//   - empty, malformed, or multi-range headers are ignored (whole object, 200);
//   - "bytes=a-b", "bytes=a-" and "bytes=-n" select one range (206);
//   - a syntactically valid range that does not overlap the object returns
//     ErrInvalidRange (416).
func parseRange(h string, size int64) (byteRange, *APIError) {
	full := byteRange{start: 0, length: size}
	spec, ok := strings.CutPrefix(strings.TrimSpace(h), "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return full, nil
	}
	first, last, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return full, nil
	}

	if first == "" { // suffix range: last n bytes
		n, err := parseNonNegative(last)
		if err != nil {
			return full, nil
		}
		if n == 0 || size == 0 {
			return byteRange{}, ErrInvalidRange
		}
		n = min(n, size)
		return byteRange{start: size - n, length: n, partial: true}, nil
	}

	start, err := parseNonNegative(first)
	if err != nil {
		return full, nil
	}
	end := int64(-1)
	if last != "" {
		if end, err = parseNonNegative(last); err != nil || end < start {
			return full, nil
		}
	}
	if start >= size {
		return byteRange{}, ErrInvalidRange
	}
	if end < 0 || end >= size {
		end = size - 1
	}
	return byteRange{start: start, length: end - start + 1, partial: true}, nil
}

func parseNonNegative(s string) (int64, error) {
	if s == "" || s[0] == '+' || s[0] == '-' {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(s, 10, 64)
}

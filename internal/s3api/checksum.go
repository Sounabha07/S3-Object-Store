package s3api

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"strings"
)

// Flexible checksums (x-amz-checksum-*). AWS SDKs send one of these by
// default on PutObject, either as a header or as an aws-chunked trailer.
// The gateway verifies whichever one is present before committing.

// crc64NVMETable uses the bit-reversed form of polynomial 0xAD93D23594C93659,
// which is what Go's reflected crc64 implementation expects.
var crc64NVMETable = crc64.MakeTable(0x9A6C9329AC4BC9B5)

var checksumAlgorithms = map[string]func() hash.Hash{
	"crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"crc64nvme": func() hash.Hash { return crc64.New(crc64NVMETable) },
	"sha1":      sha1.New,
	"sha256":    sha256.New,
}

const checksumHeaderPrefix = "x-amz-checksum-"

// checksumCheck is one expected checksum and the running hash for it.
type checksumCheck struct {
	header    string // e.g. "x-amz-checksum-crc32"
	h         hash.Hash
	want      string // base64; empty if it arrives in a trailer
	inTrailer bool
}

func (c *checksumCheck) sum() string { return base64.StdEncoding.EncodeToString(c.h.Sum(nil)) }

// newChecksumCheck returns a check for a header name such as
// "x-amz-checksum-crc32", or nil if the algorithm is unknown.
func newChecksumCheck(header string) *checksumCheck {
	header = strings.ToLower(strings.TrimSpace(header))
	algo, ok := strings.CutPrefix(header, checksumHeaderPrefix)
	if !ok {
		return nil
	}
	newHash, ok := checksumAlgorithms[algo]
	if !ok {
		return nil
	}
	return &checksumCheck{header: header, h: newHash()}
}

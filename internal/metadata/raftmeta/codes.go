package raftmeta

import (
	"errors"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

// Wire error codes. Each metadata sentinel error has a stable code so the
// client can reconstruct it with errors.Is semantics intact.
const (
	codeNotLeader      = "NotLeader"
	codeOutcomeUnknown = "OutcomeUnknown"
	codeInternal       = "Internal"
)

var errorCodes = []struct {
	code string
	err  error
}{
	{"NoSuchBucket", metadata.ErrNoSuchBucket},
	{"BucketAlreadyExists", metadata.ErrBucketAlreadyExists},
	{"BucketNotEmpty", metadata.ErrBucketNotEmpty},
	{"NoSuchKey", metadata.ErrNoSuchKey},
	{"NoSuchUpload", metadata.ErrNoSuchUpload},
	{"InvalidPart", metadata.ErrInvalidPart},
	{"InvalidPartOrder", metadata.ErrInvalidPartOrder},
	{"ShardLocationConflict", metadata.ErrShardLocationConflict},
	{"InvalidArgument", metadata.ErrInvalidArgument},
	{"NotImplemented", metadata.ErrNotImplemented},
	{codeNotLeader, metadata.ErrNotLeader},
	{"Unavailable", metadata.ErrUnavailable},
}

// errorCode returns the wire code for err, or codeInternal.
func errorCode(err error) string {
	for _, e := range errorCodes {
		if errors.Is(err, e.err) {
			return e.code
		}
	}
	return codeInternal
}

// codeError returns the sentinel for a wire code, or nil if unknown.
func codeError(code string) error {
	for _, e := range errorCodes {
		if e.code == code {
			return e.err
		}
	}
	return nil
}

// wireError is the JSON body of every error response.
type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Leader  string `json:"leader,omitempty"` // HTTP base URL of the leader, on NotLeader
}

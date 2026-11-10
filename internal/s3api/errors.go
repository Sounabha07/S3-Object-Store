package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// APIError is an S3 error response.
type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

// withMessage returns a copy of e with a more specific message.
func (e *APIError) withMessage(msg string) *APIError {
	c := *e
	c.Message = msg
	return &c
}

// S3 error codes used by the gateway. Messages follow AWS's wording.
var (
	ErrAccessDenied            = &APIError{"AccessDenied", "Access Denied", http.StatusForbidden}
	ErrBadDigest               = &APIError{"BadDigest", "The Content-MD5 or checksum you specified did not match what we received.", http.StatusBadRequest}
	ErrBucketAlreadyOwnedByYou = &APIError{"BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict}
	ErrBucketNotEmpty          = &APIError{"BucketNotEmpty", "The bucket you tried to delete is not empty", http.StatusConflict}
	ErrEntityTooLarge          = &APIError{"EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size.", http.StatusBadRequest}
	ErrIncompleteBody          = &APIError{"IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", http.StatusBadRequest}
	ErrInternal                = &APIError{"InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError}
	ErrInvalidArgument         = &APIError{"InvalidArgument", "Invalid Argument", http.StatusBadRequest}
	ErrInvalidBucketName       = &APIError{"InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest}
	ErrInvalidDigest           = &APIError{"InvalidDigest", "The Content-MD5 you specified is not valid.", http.StatusBadRequest}
	ErrInvalidRange            = &APIError{"InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable}
	ErrInvalidRequest          = &APIError{"InvalidRequest", "Invalid Request", http.StatusBadRequest}
	ErrKeyTooLong              = &APIError{"KeyTooLongError", "Your key is too long", http.StatusBadRequest}
	ErrMetadataTooLarge        = &APIError{"MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", http.StatusBadRequest}
	ErrMethodNotAllowed        = &APIError{"MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed}
	ErrMissingContentLength    = &APIError{"MissingContentLength", "You must provide the Content-Length HTTP header.", http.StatusLengthRequired}
	ErrNoSuchBucket            = &APIError{"NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound}
	ErrNoSuchKey               = &APIError{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	ErrNotImplemented          = &APIError{"NotImplemented", "A header or query you provided implies functionality that is not implemented.", http.StatusNotImplemented}
	ErrPreconditionFailed      = &APIError{"PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed}
	ErrServiceUnavailable      = &APIError{"ServiceUnavailable", "Please reduce your request rate.", http.StatusServiceUnavailable}
	ErrSHA256Mismatch          = &APIError{"XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", http.StatusBadRequest}
)

// errorResponse is the S3 XML error body.
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

// toAPIError maps internal errors to S3 errors. Unknown errors become
// InternalError so internals never leak to clients.
func toAPIError(err error) *APIError {
	var apiErr *APIError
	switch {
	case errors.As(err, &apiErr):
		return apiErr
	case errors.Is(err, metadata.ErrNoSuchBucket):
		return ErrNoSuchBucket
	case errors.Is(err, metadata.ErrNoSuchKey):
		return ErrNoSuchKey
	case errors.Is(err, metadata.ErrBucketAlreadyExists):
		return ErrBucketAlreadyOwnedByYou
	case errors.Is(err, metadata.ErrBucketNotEmpty):
		return ErrBucketNotEmpty
	case errors.Is(err, metadata.ErrInvalidArgument):
		return ErrInvalidArgument
	case errors.Is(err, metadata.ErrNotImplemented):
		return ErrNotImplemented
	case errors.Is(err, metadata.ErrUnavailable),
		errors.Is(err, storage.ErrUnavailable), // storage nodes down: possibly transient
		errors.Is(err, dataplane.ErrInsufficientNodes):
		return ErrServiceUnavailable
	case errors.Is(err, dataplane.ErrTooLarge):
		return ErrEntityTooLarge
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ErrInternal.withMessage("request cancelled")
	default:
		return ErrInternal
	}
}

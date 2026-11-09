// Package metadata defines the metadata service: buckets, objects, chunk and
// shard placement, and multipart upload state. In production it is backed by
// a 3-node Raft cluster with BoltDB as the state machine store.
package metadata

import (
	"context"
	"crypto/md5" // #nosec G501 -- S3 multipart ETag is defined over MD5
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Errors map one-to-one onto S3 error codes in the gateway.
var (
	ErrNoSuchBucket          = errors.New("metadata: no such bucket")
	ErrBucketAlreadyExists   = errors.New("metadata: bucket already exists")
	ErrBucketNotEmpty        = errors.New("metadata: bucket not empty")
	ErrNoSuchKey             = errors.New("metadata: no such key")
	ErrNoSuchUpload          = errors.New("metadata: no such upload")
	ErrInvalidPart           = errors.New("metadata: invalid part")
	ErrInvalidPartOrder      = errors.New("metadata: invalid part order")
	ErrEntityTooSmall        = errors.New("metadata: part smaller than the minimum part size")
	ErrNotLeader             = errors.New("metadata: not the raft leader")
	ErrShardLocationConflict = errors.New("metadata: shard location changed concurrently")
	ErrInvalidArgument       = errors.New("metadata: invalid argument")
	ErrNotImplemented        = errors.New("metadata: not implemented")
	// ErrUnavailable means the metadata service could not be reached or has
	// no quorum. For writes, the outcome is unknown: the change may or may
	// not have been committed.
	ErrUnavailable = errors.New("metadata: service unavailable")
)

// Scheme is the redundancy scheme of a chunk.
// {Data: 1, Parity: 2} is 3x replication; {Data: 4, Parity: 2} is RS(4,2).
type Scheme struct {
	Data   int
	Parity int
}

// Replication3 and EC42 are the two schemes the system writes by default.
// Single (one copy, no redundancy) is used only by the single-node mode.
var (
	Single       = Scheme{Data: 1, Parity: 0}
	Replication3 = Scheme{Data: 1, Parity: 2}
	EC42         = Scheme{Data: 4, Parity: 2}
)

// Total returns the number of shards per chunk.
func (s Scheme) Total() int { return s.Data + s.Parity }

// IsReplication reports whether every shard is a full copy of the chunk.
func (s Scheme) IsReplication() bool { return s.Data == 1 }

// ShardRef records where one shard of a chunk lives.
type ShardRef struct {
	Index int             // 0..Scheme.Total()-1; data shards first
	ID    storage.ShardID // SHA-256 of the shard bytes
	Size  int64           // shard size on disk (EC shards include padding)
	Node  placement.NodeID
}

// Chunk is a contiguous byte range of an object, stored as one stripe.
type Chunk struct {
	Offset int64 // logical offset within the object
	Size   int64 // logical length (excludes EC padding)
	Scheme Scheme
	Shards []ShardRef
}

// ObjectMeta is the committed state of one object.
type ObjectMeta struct {
	Bucket       string
	Key          string
	VersionID    string // internal; new per write, used for overwrite/GC safety
	Size         int64
	ETag         string // quoted S3 ETag
	ContentType  string
	UserMeta     map[string]string // x-amz-meta-* headers, lowercased keys
	Headers      map[string]string // stored system headers (Cache-Control, Content-Encoding, ...), canonical names
	LastModified time.Time
	Chunks       []Chunk
}

// BucketInfo describes a bucket.
type BucketInfo struct {
	Name      string
	CreatedAt time.Time
}

// ListOptions are the ListObjectsV2 query parameters.
type ListOptions struct {
	Prefix            string
	Delimiter         string
	MaxKeys           int // 0 means the S3 default of 1000
	StartAfter        string
	ContinuationToken string
}

// ObjectSummary is one entry in a listing.
type ObjectSummary struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// ListResult is one page of ListObjectsV2.
type ListResult struct {
	Objects               []ObjectSummary
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
}

// UploadMeta is the object-level metadata supplied at CreateMultipartUpload.
type UploadMeta struct {
	ContentType string
	UserMeta    map[string]string
	Headers     map[string]string
}

// UploadInfo describes an in-progress multipart upload.
type UploadInfo struct {
	UploadID  string
	Bucket    string
	Key       string
	Initiated time.Time
	Meta      UploadMeta
}

// CompleteRequest is the input of CompleteMultipart.
type CompleteRequest struct {
	Parts       []CompletedPart
	VersionID   string // version of the resulting object, chosen by the caller
	MinPartSize int64  // every part but the last must be at least this big
}

// PartMeta is the committed state of one uploaded part.
type PartMeta struct {
	PartNumber   int // 1..10000
	Size         int64
	ETag         string // quoted MD5 of the part
	LastModified time.Time
	Chunks       []Chunk // offsets relative to the start of the part
}

// ListPartsOptions are the ListParts query parameters.
type ListPartsOptions struct {
	MaxParts         int
	PartNumberMarker int
}

// ListPartsResult is one page of ListParts.
type ListPartsResult struct {
	Bucket               string
	Key                  string
	UploadID             string
	Parts                []PartMeta
	IsTruncated          bool
	NextPartNumberMarker int
}

// CompletedPart is one entry of the CompleteMultipartUpload request body.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// MultipartETag computes S3's ETag for a multipart object: the MD5 of the
// concatenated binary MD5s of the parts, then "-N". Part ETags are the
// quoted hex MD5s returned by UploadPart.
func MultipartETag(parts []CompletedPart) (string, error) {
	h := md5.New() // #nosec G401 -- S3 defines the multipart ETag as MD5 of MD5s
	for _, p := range parts {
		sum, err := hex.DecodeString(strings.Trim(p.ETag, `"`))
		if err != nil || len(sum) != md5.Size {
			return "", fmt.Errorf("%w: part %d ETag %q", ErrInvalidPart, p.PartNumber, p.ETag)
		}
		h.Write(sum)
	}
	return fmt.Sprintf(`"%x-%d"`, h.Sum(nil), len(parts)), nil
}

// MaxPartNumber is the highest part number S3 allows.
const MaxPartNumber = 10000

// ShardLocationUpdate moves one shard to a new node (repair/rebalance).
// It is applied only if the shard is still recorded on From, so a stale
// repair cannot overwrite a newer location.
type ShardLocationUpdate struct {
	Bucket     string
	Key        string
	VersionID  string
	ChunkIndex int
	ShardIndex int
	From       placement.NodeID
	To         placement.NodeID
}

// Metadata is the metadata service API used by gateways, repair and GC.
//
// All writes are linearizable. Implementations must be safe for concurrent use.
type Metadata interface {
	CreateBucket(ctx context.Context, name string) error
	DeleteBucket(ctx context.Context, name string) error // ErrBucketNotEmpty
	HeadBucket(ctx context.Context, name string) (BucketInfo, error)
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	// PutObject commits obj, atomically replacing any existing version.
	// Callers must only invoke it after every shard is durably stored.
	PutObject(ctx context.Context, obj ObjectMeta) error
	GetObject(ctx context.Context, bucket, key string) (ObjectMeta, error)
	// DeleteObject removes the key. Deleting a missing key succeeds (S3 semantics).
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListResult, error)

	// CreateMultipart starts an upload (ErrNoSuchBucket if the bucket is gone).
	CreateMultipart(ctx context.Context, bucket, key string, m UploadMeta) (uploadID string, err error)
	// GetUpload returns an in-progress upload (ErrNoSuchUpload).
	GetUpload(ctx context.Context, uploadID string) (UploadInfo, error)
	// PutPart records a part, replacing any earlier part with its number.
	PutPart(ctx context.Context, uploadID string, p PartMeta) error
	ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (ListPartsResult, error)
	// CompleteMultipart assembles the listed parts into an object. It
	// fails with ErrInvalidPartOrder, ErrInvalidPart or ErrEntityTooSmall
	// and changes nothing in that case. The returned ObjectMeta carries at
	// least Bucket, Key, VersionID and ETag.
	CompleteMultipart(ctx context.Context, uploadID string, req CompleteRequest) (ObjectMeta, error)
	AbortMultipart(ctx context.Context, uploadID string) error

	// UpdateShardLocation is used by repair and rebalancing.
	UpdateShardLocation(ctx context.Context, u ShardLocationUpdate) error
	// WalkChunks visits every chunk referenced by committed objects. Used by
	// repair, rebalancing and the GC mark phase.
	WalkChunks(ctx context.Context, fn func(bucket, key, versionID string, chunkIndex int, c Chunk) error) error
	// WalkPartChunks visits every chunk of in-progress multipart parts, so
	// GC never deletes data an upload will still commit.
	WalkPartChunks(ctx context.Context, fn func(uploadID string, partNumber int, c Chunk) error) error
}

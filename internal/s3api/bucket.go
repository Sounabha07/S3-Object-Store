package s3api

import (
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request, req *request) {
	req.op = "ListBuckets"
	buckets, err := s.meta.ListBuckets(r.Context())
	if err != nil {
		s.fail(w, r, req, err)
		return
	}
	res := listAllMyBucketsResult{Xmlns: s3Namespace, Owner: defaultOwner, Buckets: []bucketXMLDoc{}}
	for _, b := range buckets {
		res.Buckets = append(res.Buckets, bucketXMLDoc{Name: b.Name, CreationDate: xmlTime(b.CreatedAt)})
	}
	s.writeXML(w, http.StatusOK, res)
}

func (s *Server) createBucket(w http.ResponseWriter, r *http.Request, req *request) {
	req.op = "CreateBucket"
	if !validBucketName(req.bucket) {
		s.writeError(w, r, req, ErrInvalidBucketName)
		return
	}
	// The optional CreateBucketConfiguration body only names a region; this
	// store has one region, so the body is read and ignored.
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 64<<10))
	if err := s.meta.CreateBucket(r.Context(), req.bucket); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.Header().Set("Location", "/"+req.bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, req *request) {
	req.op = "DeleteBucket"
	if err := s.meta.DeleteBucket(r.Context(), req.bucket); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, req *request) {
	req.op = "HeadBucket"
	if _, err := s.meta.HeadBucket(r.Context(), req.bucket); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.Header().Set("x-amz-bucket-region", "us-east-1")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getBucketLocation(w http.ResponseWriter, r *http.Request, req *request) {
	req.op = "GetBucketLocation"
	if _, err := s.meta.HeadBucket(r.Context(), req.bucket); err != nil {
		s.fail(w, r, req, err)
		return
	}
	// An empty LocationConstraint means us-east-1.
	s.writeXML(w, http.StatusOK, locationConstraint{Xmlns: s3Namespace})
}

func (s *Server) listObjectsV2(w http.ResponseWriter, r *http.Request, req *request) {
	req.op = "ListObjectsV2"
	q := r.URL.Query()
	opts := metadata.ListOptions{
		Prefix:            q.Get("prefix"),
		Delimiter:         q.Get("delimiter"),
		StartAfter:        q.Get("start-after"),
		ContinuationToken: q.Get("continuation-token"),
		MaxKeys:           1000,
	}
	if q.Has("max-keys") {
		n, err := strconv.Atoi(q.Get("max-keys"))
		if err != nil || n < 0 {
			s.writeError(w, r, req, ErrInvalidArgument.withMessage("Provided max-keys not an integer or within integer range"))
			return
		}
		opts.MaxKeys = min(n, 1000)
	}
	encoding := q.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		s.writeError(w, r, req, ErrInvalidArgument.withMessage("Invalid Encoding Method specified in Request"))
		return
	}
	if q.Has("continuation-token") && opts.ContinuationToken == "" {
		s.writeError(w, r, req, ErrInvalidArgument.withMessage("The continuation token provided is incorrect"))
		return
	}

	var res metadata.ListResult
	if opts.MaxKeys == 0 {
		// S3 returns an empty, non-truncated page for max-keys=0, but still
		// reports a missing bucket.
		if _, err := s.meta.HeadBucket(r.Context(), req.bucket); err != nil {
			s.fail(w, r, req, err)
			return
		}
	} else {
		var err error
		res, err = s.meta.ListObjects(r.Context(), req.bucket, opts)
		if err != nil {
			apiErr := toAPIError(err)
			if apiErr == ErrInvalidArgument {
				apiErr = ErrInvalidArgument.withMessage("The continuation token provided is incorrect")
			}
			s.writeError(w, r, req, apiErr)
			return
		}
	}

	enc := func(s string) string { return s }
	if encoding == "url" {
		enc = url.QueryEscape
	}
	out := listBucketV2Result{
		Xmlns:                 s3Namespace,
		Name:                  req.bucket,
		Prefix:                enc(opts.Prefix),
		Delimiter:             enc(opts.Delimiter),
		MaxKeys:               opts.MaxKeys,
		KeyCount:              len(res.Objects) + len(res.CommonPrefixes),
		IsTruncated:           res.IsTruncated,
		ContinuationToken:     opts.ContinuationToken,
		NextContinuationToken: res.NextContinuationToken,
		StartAfter:            enc(opts.StartAfter),
		EncodingType:          encoding,
	}
	fetchOwner := q.Get("fetch-owner") == "true"
	for _, o := range res.Objects {
		x := objectXML{
			Key: enc(o.Key), LastModified: xmlTime(o.LastModified), ETag: o.ETag,
			Size: o.Size, StorageClass: "STANDARD",
		}
		if fetchOwner {
			x.Owner = &defaultOwner
		}
		out.Contents = append(out.Contents, x)
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: enc(p)})
	}
	s.writeXML(w, http.StatusOK, out)
}

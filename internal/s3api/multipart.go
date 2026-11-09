package s3api

import (
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listPartsResult struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	Xmlns                string    `xml:"xmlns,attr"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	StorageClass         string    `xml:"StorageClass"`
	Initiator            owner     `xml:"Initiator"`
	Owner                owner     `xml:"Owner"`
	Parts                []partXML `xml:"Part"`
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

// maxCompleteBody bounds the CompleteMultipartUpload XML (10,000 parts of
// about 100 bytes each).
const maxCompleteBody = 2 << 20

func (s *Server) routeMultipart(w http.ResponseWriter, r *http.Request, req *request, q url.Values) {
	uploadID := q.Get("uploadId")
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.createMultipart(w, r, req)
	case r.Method == http.MethodPut && q.Has("partNumber") && uploadID != "":
		s.uploadPart(w, r, req, uploadID, q.Get("partNumber"))
	case r.Method == http.MethodPost && uploadID != "":
		s.completeMultipart(w, r, req, uploadID)
	case r.Method == http.MethodDelete && uploadID != "":
		s.abortMultipart(w, r, req, uploadID)
	case r.Method == http.MethodGet && uploadID != "":
		s.listParts(w, r, req, uploadID)
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && q.Has("partNumber"):
		s.writeError(w, r, req, ErrNotImplemented.withMessage("GetObject by partNumber is not implemented."))
	case q.Has("partNumber"):
		s.writeError(w, r, req, ErrInvalidArgument.withMessage("partNumber requires uploadId."))
	default:
		s.writeError(w, r, req, ErrMethodNotAllowed)
	}
}

func (s *Server) createMultipart(w http.ResponseWriter, r *http.Request, req *request) {
	userMeta, apiErr := parseUserMeta(r.Header)
	if apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return
	}
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = "binary/octet-stream"
	}
	id, err := s.meta.CreateMultipart(r.Context(), req.bucket, req.key, metadata.UploadMeta{
		ContentType: ct, UserMeta: userMeta, Headers: parseStoredHeaders(r.Header),
	})
	if err != nil {
		s.fail(w, r, req, err)
		return
	}
	s.writeXML(w, http.StatusOK, initiateMultipartUploadResult{Xmlns: s3Namespace, Bucket: req.bucket, Key: req.key, UploadID: id})
}

// upload loads an upload and checks it belongs to the request's object.
func (s *Server) upload(w http.ResponseWriter, r *http.Request, req *request, uploadID string) (metadata.UploadInfo, bool) {
	up, err := s.meta.GetUpload(r.Context(), uploadID)
	if err == nil && (up.Bucket != req.bucket || up.Key != req.key) {
		err = metadata.ErrNoSuchUpload
	}
	if err != nil {
		s.fail(w, r, req, err)
		return up, false
	}
	return up, true
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, req *request, uploadID, partNumber string) {
	n, err := strconv.Atoi(partNumber)
	if err != nil || n < 1 || n > metadata.MaxPartNumber {
		s.writeError(w, r, req, ErrInvalidArgument.withMessage("Part number must be an integer between 1 and 10000, inclusive."))
		return
	}
	if r.Header.Get("x-amz-copy-source") != "" {
		s.writeError(w, r, req, ErrNotImplemented.withMessage("UploadPartCopy is not implemented."))
		return
	}
	if _, ok := s.upload(w, r, req, uploadID); !ok {
		return
	}
	// Each part gets its own placement key: parts are written independently
	// and may be retried or replaced before completion.
	res, in, ok := s.ingest(w, r, req, dataplane.ObjectKey(req.bucket, req.key, uploadID+"/part-"+strconv.Itoa(n)))
	if !ok {
		return
	}
	part := metadata.PartMeta{
		PartNumber: n, Size: res.Size, ETag: `"` + hex.EncodeToString(res.MD5[:]) + `"`,
		LastModified: s.now(), Chunks: res.Chunks,
	}
	if err := s.meta.PutPart(r.Context(), uploadID, part); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.Header().Set("ETag", part.ETag)
	for _, c := range in.checksums {
		w.Header().Set(c.header, c.sum())
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) completeMultipart(w http.ResponseWriter, r *http.Request, req *request, uploadID string) {
	if _, ok := s.upload(w, r, req, uploadID); !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCompleteBody+1))
	if err != nil || len(body) > maxCompleteBody {
		s.writeError(w, r, req, ErrMalformedXML)
		return
	}
	var doc completeMultipartUpload
	if err := xml.Unmarshal(body, &doc); err != nil || len(doc.Parts) == 0 {
		s.writeError(w, r, req, ErrMalformedXML)
		return
	}
	parts := make([]metadata.CompletedPart, len(doc.Parts))
	for i, p := range doc.Parts {
		parts[i] = metadata.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
	}
	obj, err := s.meta.CompleteMultipart(r.Context(), uploadID, metadata.CompleteRequest{
		Parts: parts, VersionID: newVersionID(), MinPartSize: s.minPartSize,
	})
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			s.writeError(w, r, req, apiErr)
			return
		}
		s.fail(w, r, req, err)
		return
	}
	s.writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns: s3Namespace, Location: fmt.Sprintf("/%s/%s", req.bucket, req.key),
		Bucket: req.bucket, Key: req.key, ETag: obj.ETag,
	})
}

func (s *Server) abortMultipart(w http.ResponseWriter, r *http.Request, req *request, uploadID string) {
	if _, ok := s.upload(w, r, req, uploadID); !ok {
		return
	}
	if err := s.meta.AbortMultipart(r.Context(), uploadID); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, req *request, uploadID string) {
	if _, ok := s.upload(w, r, req, uploadID); !ok {
		return
	}
	q := r.URL.Query()
	maxParts, marker := 1000, 0
	if v := q.Get("max-parts"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			s.writeError(w, r, req, ErrInvalidArgument.withMessage("Invalid max-parts"))
			return
		}
		maxParts = min(n, 1000)
	}
	if v := q.Get("part-number-marker"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			s.writeError(w, r, req, ErrInvalidArgument.withMessage("Invalid part-number-marker"))
			return
		}
		marker = n
	}
	res, err := s.meta.ListParts(r.Context(), uploadID, metadata.ListPartsOptions{MaxParts: maxParts, PartNumberMarker: marker})
	if err != nil {
		s.fail(w, r, req, err)
		return
	}
	out := listPartsResult{
		Xmlns: s3Namespace, Bucket: req.bucket, Key: req.key, UploadID: uploadID,
		PartNumberMarker: marker, NextPartNumberMarker: res.NextPartNumberMarker, MaxParts: maxParts,
		IsTruncated: res.IsTruncated, StorageClass: "STANDARD", Initiator: defaultOwner, Owner: defaultOwner,
	}
	for _, p := range res.Parts {
		out.Parts = append(out.Parts, partXML{PartNumber: p.PartNumber, LastModified: xmlTime(p.LastModified), ETag: p.ETag, Size: p.Size})
	}
	s.writeXML(w, http.StatusOK, out)
}

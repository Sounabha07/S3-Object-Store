package s3api

import (
	"encoding/xml"
	"time"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// iso8601 is the timestamp format S3 uses in XML bodies.
const iso8601 = "2006-01-02T15:04:05.000Z"

func xmlTime(t time.Time) string { return t.UTC().Format(iso8601) }

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

var defaultOwner = owner{ID: "s3os", DisplayName: "s3os"}

type listAllMyBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Owner   owner          `xml:"Owner"`
	Buckets []bucketXMLDoc `xml:"Buckets>Bucket"`
}

type bucketXMLDoc struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type listBucketV2Result struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	Contents              []objectXML    `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *owner `xml:"Owner,omitempty"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

package server

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"maxstore/internal/store"
)

const namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

func xmlReply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(v)
}
func s3Error(w http.ResponseWriter, status int, code, message string) {
	xmlReply(w, status, struct {
		XMLName xml.Name `xml:"Error"`
		Code    string
		Message string
	}{Code: code, Message: message})
}
func objectHeaders(w http.ResponseWriter, o store.Object) {
	for key, value := range o.Metadata {
		w.Header().Set("X-Amz-Meta-"+key, value)
	}
	for key, value := range o.Headers {
		w.Header().Set(key, value)
	}
	for key, value := range o.Checksums {
		w.Header().Set(key, value)
	}
	w.Header().Set("Content-Type", o.Type)
	w.Header().Set("Content-Length", strconv.FormatInt(o.Size, 10))
	w.Header().Set("ETag", `"`+o.ETag+`"`)
	w.Header().Set("Last-Modified", o.Created.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
}
func (s *Server) S3() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e := s.verify(r); e != nil {
			s3Error(w, 403, "SignatureDoesNotMatch", e.Error())
			return
		}
		payload := r.Header.Get("X-Amz-Content-Sha256")
		if strings.HasPrefix(payload, "STREAMING-") || r.Header.Get("Content-Encoding") == "aws-chunked" {
			s3Error(w, 501, "NotImplemented", "AWS streaming signatures are not supported; use a fixed-length PUT")
			return
		}
		if r.Header.Get("X-Amz-Server-Side-Encryption") != "" || r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != "" || (r.Header.Get("X-Amz-Acl") != "" && r.Header.Get("X-Amz-Acl") != "private") {
			s3Error(w, 501, "NotImplemented", "AWS encryption and public ACL modes are not supported")
			return
		}
		q := r.URL.Query()
		for _, param := range []string{"versionId", "versioning", "versions", "acl", "policy", "lifecycle", "cors", "replication", "website", "notification", "logging", "encryption", "object-lock", "retention", "legal-hold", "restore", "select", "select-type", "accelerate", "analytics", "inventory", "metrics", "ownershipControls", "publicAccessBlock", "requestPayment", "torrent", "attributes"} {
			if q.Has(param) {
				s3Error(w, 501, "NotImplemented", "This S3 operation is not supported")
				return
			}
		}
		if e := validateTagHeader(r.Header.Get("X-Amz-Tagging")); e != nil {
			s3Failure(w, e)
			return
		}
		if unsupportedChecksum(r) {
			s3Error(w, 501, "NotImplemented", "CRC64NVME is not supported; select CRC32, CRC32C, SHA1 or SHA256")
			return
		}

		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		bucket := parts[0]
		key := ""
		if len(parts) == 2 {
			key = parts[1]
		}
		fail := func(e error) {
			code := "InternalError"
			switch errorStatus(e) {
			case 404:
				if key == "" {
					code = "NoSuchBucket"
				} else {
					code = "NoSuchKey"
				}
			case 400:
				code = "InvalidArgument"
			case 412:
				code = "PreconditionFailed"
			case 409:
				code = "BucketAlreadyExists"
			case 413:
				code = "EntityTooLarge"
			}
			s3Error(w, errorStatus(e), code, e.Error())
		}
		if q.Has("tagging") {
			s.tagging(w, r, bucket, key)
			return
		}
		if r.Header.Get("X-Amz-Copy-Source") != "" && key != "" {
			s.copyObject(w, r, bucket, key)
			return
		}
		if q.Has("delete") && key == "" {
			s.deleteObjects(w, r, bucket)
			return
		}
		if q.Has("uploads") || q.Has("uploadId") {
			s.multipart(w, r, bucket, key, payload)
			return
		}
		if key == "" && (r.Method == "PUT" || r.Method == "DELETE") {
			raw, e := io.ReadAll(io.LimitReader(r.Body, 65537))
			if e != nil || len(raw) > 65536 {
				s3Error(w, 400, "InvalidRequest", "Invalid request body")
				return
			}
			if payload != "" && payload != "UNSIGNED-PAYLOAD" && hash(string(raw)) != payload {
				s3Error(w, 400, "XAmzContentSHA256Mismatch", "Payload checksum mismatch")
				return
			}
		}
		if bucket == "" && r.Method == "GET" {
			b, e := s.Store.Buckets()
			if e != nil {
				fail(e)
				return
			}
			type entry struct {
				Name         string
				CreationDate string
			}
			out := struct {
				XMLName xml.Name `xml:"ListAllMyBucketsResult"`
				XMLNS   string   `xml:"xmlns,attr"`
				Owner   struct {
					ID          string
					DisplayName string
				}
				Buckets []entry `xml:"Buckets>Bucket"`
			}{XMLNS: namespace}
			out.Owner.ID = "maxstore"
			out.Owner.DisplayName = "MaxStore"
			for _, v := range b {
				out.Buckets = append(out.Buckets, entry{v.Name, v.Created.Format(time.RFC3339)})
			}
			xmlReply(w, 200, out)
			return
		}
		if key == "" {
			switch r.Method {
			case "PUT":
				p := r.Header.Get("X-Maxstore-Provider")
				if p == "" {
					p = s.DefaultProvider
				}
				if e := s.Store.CreateBucket(bucket, p); e != nil {
					fail(e)
					return
				}
				w.Header().Set("Location", "/"+bucket)
				w.WriteHeader(200)
				return
			case "DELETE":
				if e := s.Store.DeleteBucket(bucket); e != nil {
					if errorStatus(e) == 409 {
						s3Error(w, 409, "BucketNotEmpty", "Bucket is not empty")
					} else {
						fail(e)
					}
					return
				}
				w.WriteHeader(204)
				return
			case "HEAD":
				if _, e := s.Store.Bucket(bucket); e != nil {
					fail(e)
					return
				}
				w.WriteHeader(200)
				return
			case "GET":
				if q.Has("location") {
					if _, e := s.Store.Bucket(bucket); e != nil {
						fail(e)
						return
					}
					xmlReply(w, 200, struct {
						XMLName xml.Name `xml:"LocationConstraint"`
						XMLNS   string   `xml:"xmlns,attr"`
					}{XMLNS: namespace})
					return
				}
				s.listObjects(w, r, bucket)
				return
			}
		} else {
			switch r.Method {
			case "PUT":

				if s.Store.MaxSize > 0 {
					r.Body = http.MaxBytesReader(w, r.Body, s.Store.MaxSize+1)
				}
				p := newPayload(r)
				opts := requestOptions(r)
				opts.Validate = p.Verify
				opts.Checksums = p.Checksums
				o, e := s.Store.Put(r.Context(), bucket, key, r.Header.Get("Content-Type"), p, payload, opts)
				if e != nil {
					fail(e)
					return
				}
				w.Header().Set("ETag", `"`+o.ETag+`"`)
				w.WriteHeader(200)
				return
			case "GET", "HEAD":
				s.serveObject(w, r, bucket, key, false)
				return
			case "DELETE":
				if _, e := s.Store.Bucket(bucket); e != nil {
					fail(e)
					return
				}
				if e := s.Store.Delete(bucket, key); e != nil {
					fail(e)
					return
				}
				w.WriteHeader(204)
				return
			}
		}
		s3Error(w, 405, "MethodNotAllowed", "Method not allowed")
	})
}

type content struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	StorageClass string
}
type commonPrefix struct{ Prefix string }

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	v2 := q.Get("list-type") == "2"
	if q.Has("list-type") && !v2 {
		s3Error(w, 400, "InvalidArgument", "Unsupported list-type")
		return
	}
	objects, e := s.Store.Objects(bucket, q.Get("prefix"))
	if e != nil {
		s3Error(w, errorStatus(e), "NoSuchBucket", e.Error())
		return
	}
	limit := 1000
	if q.Has("max-keys") {
		limit, e = strconv.Atoi(q.Get("max-keys"))
		if e != nil || limit < 0 || limit > 1000 {
			s3Error(w, 400, "InvalidArgument", "max-keys must be between 0 and 1000")
			return
		}
	}
	after := q.Get("marker")
	if v2 {
		after = q.Get("start-after")
		if token := q.Get("continuation-token"); token != "" {
			b, e := base64.RawURLEncoding.DecodeString(token)
			if e != nil {
				s3Error(w, 400, "InvalidArgument", "Invalid continuation token")
				return
			}
			after = string(b)
		}
	}
	type item struct {
		key string
		obj *store.Object
	}
	items := []item{}
	seen := map[string]bool{}
	for i := range objects {
		o := &objects[i]
		key := o.Key
		prefix := false
		if d := q.Get("delimiter"); d != "" {
			rest := strings.TrimPrefix(key, q.Get("prefix"))
			if idx := strings.Index(rest, d); idx >= 0 {
				key = q.Get("prefix") + rest[:idx+len(d)]
				prefix = true
			}
		}
		if key <= after || seen[key] {
			continue
		}
		seen[key] = true
		if prefix {
			items = append(items, item{key, nil})
		} else {
			items = append(items, item{key, o})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })
	truncated := len(items) > limit && limit > 0
	if len(items) > limit {
		items = items[:limit]
	}
	out := struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		XMLNS                 string   `xml:"xmlns,attr"`
		Name                  string
		Prefix                string
		Delimiter             string `xml:",omitempty"`
		MaxKeys               int
		KeyCount              *int `xml:",omitempty"`
		IsTruncated           bool
		ContinuationToken     string  `xml:",omitempty"`
		NextContinuationToken string  `xml:",omitempty"`
		StartAfter            string  `xml:",omitempty"`
		Marker                *string `xml:",omitempty"`
		NextMarker            string  `xml:",omitempty"`
		Contents              []content
		CommonPrefixes        []commonPrefix
		EncodingType          string `xml:",omitempty"`
	}{XMLNS: namespace, Name: bucket, Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), MaxKeys: limit, IsTruncated: truncated}
	for _, v := range items {
		if v.obj == nil {
			out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{v.key})
		} else {
			o := v.obj
			out.Contents = append(out.Contents, content{o.Key, o.Created.Format(time.RFC3339Nano), fmt.Sprintf(`"%s"`, o.ETag), o.Size, "STANDARD"})
		}
	}
	if v2 {
		n := len(items)
		out.KeyCount = &n
		out.ContinuationToken = q.Get("continuation-token")
		out.StartAfter = q.Get("start-after")
		if truncated {
			out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(items[len(items)-1].key))
		}
	} else {
		out.Marker = &after
		if truncated {
			out.NextMarker = items[len(items)-1].key
		}
	}
	if q.Get("encoding-type") == "url" {
		out.EncodingType = "url"
		out.Prefix = encode(out.Prefix)
		out.Delimiter = encode(out.Delimiter)
		out.StartAfter = encode(out.StartAfter)
		out.NextMarker = encode(out.NextMarker)
		if out.Marker != nil {
			x := encode(*out.Marker)
			out.Marker = &x
		}
		for i := range out.Contents {
			out.Contents[i].Key = encode(out.Contents[i].Key)
		}
		for i := range out.CommonPrefixes {
			out.CommonPrefixes[i].Prefix = encode(out.CommonPrefixes[i].Prefix)
		}
	}
	xmlReply(w, 200, out)
}

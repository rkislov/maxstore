package server

import (
	"encoding/xml"
	"errors"
	"io"
	"maxstore/internal/store"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func requestOptions(r *http.Request) store.ObjectOptions {
	opts := store.ObjectOptions{Metadata: map[string]string{}, Headers: map[string]string{}, Checksums: map[string]string{}, IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match")}
	for key, values := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			opts.Metadata[strings.ToLower(strings.TrimPrefix(strings.ToLower(key), "x-amz-meta-"))] = strings.Join(values, ",")
		}
	}
	for _, key := range []string{"Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "Expires"} {
		if value := r.Header.Get(key); value != "" {
			opts.Headers[key] = value
		}
	}
	if strings.Contains(opts.Headers["Content-Encoding"], "aws-chunked") {
		encodings := []string{}
		for _, enc := range strings.Split(opts.Headers["Content-Encoding"], ",") {
			if strings.TrimSpace(enc) != "aws-chunked" {
				encodings = append(encodings, strings.TrimSpace(enc))
			}
		}
		if len(encodings) == 0 {
			delete(opts.Headers, "Content-Encoding")
		} else {
			opts.Headers["Content-Encoding"] = strings.Join(encodings, ",")
		}
	}
	if tags, e := url.ParseQuery(r.Header.Get("X-Amz-Tagging")); e == nil && len(tags) > 0 {
		opts.Tags = map[string]string{}
		for k, v := range tags {
			opts.Tags[k] = v[0]
		}
	}
	return opts
}
func s3Failure(w http.ResponseWriter, e error) {
	code := "InternalError"
	switch {
	case errors.Is(e, store.ErrPrecondition):
		code = "PreconditionFailed"
	case errors.Is(e, store.ErrDigest):
		code = "BadDigest"
	case errors.Is(e, store.ErrNotFound):
		code = "NoSuchKey"
	case errors.Is(e, store.ErrInvalid), errors.Is(e, store.ErrPart):
		code = "InvalidArgument"
	case errors.Is(e, store.ErrTooLarge):
		code = "EntityTooLarge"
	}
	s3Error(w, errorStatus(e), code, e.Error())
}
func parseCopySource(value string) (string, string, error) {
	decoded, e := url.PathUnescape(value)
	if e != nil {
		return "", "", store.ErrInvalid
	}
	if strings.Contains(decoded, "?") {
		return "", "", errors.New("versioned copies are not supported")
	}
	parts := strings.SplitN(strings.TrimPrefix(decoded, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", store.ErrInvalid
	}
	return parts[0], parts[1], nil
}
func etagMatches(value, etag string) bool {
	for _, candidate := range strings.Split(value, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.Trim(candidate, `"`) == etag {
			return true
		}
	}
	return false
}
func copyConditions(r *http.Request, o store.Object) error {
	if value := r.Header.Get("X-Amz-Copy-Source-If-Match"); value != "" && !etagMatches(value, o.ETag) {
		return store.ErrPrecondition
	}
	if value := r.Header.Get("X-Amz-Copy-Source-If-None-Match"); value != "" && etagMatches(value, o.ETag) {
		return store.ErrPrecondition
	}
	for _, spec := range []struct {
		key      string
		modified bool
	}{{"X-Amz-Copy-Source-If-Modified-Since", true}, {"X-Amz-Copy-Source-If-Unmodified-Since", false}} {
		if !spec.modified && r.Header.Get("X-Amz-Copy-Source-If-Match") != "" {
			continue
		}
		if value := r.Header.Get(spec.key); value != "" {
			t, e := http.ParseTime(value)
			if e != nil {
				return store.ErrInvalid
			}
			modified := o.Created.After(t.Add(time.Second - time.Nanosecond))
			if modified != spec.modified {
				return store.ErrPrecondition
			}
		}
	}
	return nil
}
func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Method != "PUT" {
		s3Error(w, 405, "MethodNotAllowed", "Copy requires PUT")
		return
	}
	body, e := readS3Small(r, 4096)
	if e != nil || len(body) != 0 {
		if e == nil {
			e = store.ErrInvalid
		}
		s3Failure(w, e)
		return
	}
	sourceBucket, sourceKey, e := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if e != nil {
		s3Failure(w, e)
		return
	}
	reader, source, e := s.Store.OpenReader(r.Context(), sourceBucket, sourceKey)
	if e != nil {
		s3Failure(w, e)
		return
	}
	defer reader.Close()
	if e = copyConditions(r, source); e != nil {
		s3Failure(w, e)
		return
	}
	opts := requestOptions(r)
	if id := r.URL.Query().Get("uploadId"); id != "" {
		upload, e := s.Store.Upload(id)
		if e != nil || upload.Bucket != bucket || upload.Key != key || upload.Mode != "s3" {
			s3Error(w, 404, "NoSuchUpload", "Upload not found")
			return
		}
		number, e := strconv.Atoi(r.URL.Query().Get("partNumber"))
		if e != nil {
			s3Failure(w, store.ErrPart)
			return
		}
		var input io.Reader = reader
		if value := r.Header.Get("X-Amz-Copy-Source-Range"); value != "" {
			bounds := strings.SplitN(strings.TrimPrefix(value, "bytes="), "-", 2)
			if !strings.HasPrefix(value, "bytes=") || len(bounds) != 2 {
				s3Failure(w, store.ErrInvalid)
				return
			}
			start, e1 := strconv.ParseInt(bounds[0], 10, 64)
			end, e2 := strconv.ParseInt(bounds[1], 10, 64)
			if e1 != nil || e2 != nil || start < 0 || end < start || end >= source.Size {
				s3Error(w, 416, "InvalidRange", "Invalid copy range")
				return
			}
			reader.PrefetchThrough(end)
			if _, e = reader.Seek(start, io.SeekStart); e != nil {
				s3Failure(w, e)
				return
			}
			input = io.LimitReader(reader, end-start+1)
		} else if source.Size > store.MaxS3PartSize {
			s3Failure(w, store.ErrTooLarge)
			return
		}
		part, e := s.Store.UploadPart(r.Context(), id, number, input, "")
		if e != nil {
			s3Failure(w, e)
			return
		}
		xmlReply(w, 200, struct {
			XMLName            xml.Name `xml:"CopyPartResult"`
			LastModified, ETag string
		}{LastModified: part.Modified.Format(time.RFC3339Nano), ETag: `"` + part.ETag + `"`})
		return
	}
	if source.Size > 5<<30 {
		s3Error(w, 400, "EntityTooLarge", "Use multipart copy for objects larger than 5 GiB")
		return
	}
	ctype := r.Header.Get("Content-Type")
	directive := r.Header.Get("X-Amz-Metadata-Directive")
	if directive == "" || directive == "COPY" {
		if r.Header.Get("X-Amz-Tagging-Directive") != "REPLACE" {
			opts.Tags = source.Tags
		}
		opts.Metadata = source.Metadata
		opts.Headers = source.Headers
		ctype = source.Type
	} else if directive != "REPLACE" {
		s3Failure(w, store.ErrInvalid)
		return
	}
	copied, e := s.Store.Put(r.Context(), bucket, key, ctype, reader, "", opts)
	if e != nil {
		s3Failure(w, e)
		return
	}
	xmlReply(w, 200, struct {
		XMLName            xml.Name `xml:"CopyObjectResult"`
		LastModified, ETag string
	}{LastModified: copied.Created.Format(time.RFC3339Nano), ETag: `"` + copied.ETag + `"`})
}
func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != "POST" {
		s3Error(w, 405, "MethodNotAllowed", "DeleteObjects requires POST")
		return
	}
	if _, e := s.Store.Bucket(bucket); e != nil {
		s3Error(w, 404, "NoSuchBucket", e.Error())
		return
	}
	raw, e := readS3Small(r, 1<<20)
	if e != nil {
		s3Failure(w, e)
		return
	}
	var body struct {
		XMLName xml.Name `xml:"Delete"`
		Objects []struct {
			Key       string
			VersionID string `xml:"VersionId"`
		} `xml:"Object"`
		Quiet bool
	}
	if e = xml.Unmarshal(raw, &body); e != nil || len(body.Objects) == 0 || len(body.Objects) > 1000 {
		s3Error(w, 400, "MalformedXML", "Expected 1–1000 objects")
		return
	}
	type deleted struct{ Key string }
	type failure struct{ Key, Code, Message string }
	result := struct {
		XMLName xml.Name  `xml:"DeleteResult"`
		XMLNS   string    `xml:"xmlns,attr"`
		Deleted []deleted `xml:"Deleted"`
		Errors  []failure `xml:"Error"`
	}{XMLNS: namespace}
	for _, object := range body.Objects {
		if object.VersionID != "" {
			result.Errors = append(result.Errors, failure{object.Key, "NotImplemented", "Versioned deletion is not supported"})
			continue
		}
		if e = s.Store.Delete(bucket, object.Key); e != nil {
			result.Errors = append(result.Errors, failure{object.Key, "InvalidArgument", e.Error()})
		} else if !body.Quiet {
			result.Deleted = append(result.Deleted, deleted{object.Key})
		}
	}
	xmlReply(w, 200, result)
}

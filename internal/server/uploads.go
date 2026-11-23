package server

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"maxstore/internal/store"
)

func (s *Server) uploadAPI(w http.ResponseWriter, r *http.Request, path string) {
	if path == "uploads" && r.Method == "POST" {
		var v struct {
			Bucket, Key, Type string
			Size              int64
			Parallel          bool
		}
		if e := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&v); e != nil {
			jsonReply(w, 400, map[string]string{"error": "Некорректный JSON"})
			return
		}
		mode := "resumable"
		if v.Parallel {
			mode = "parallel"
		}
		u, e := s.Store.CreateUpload(v.Bucket, v.Key, v.Type, mode, v.Size)
		if e != nil {
			apiError(w, e)
			return
		}
		jsonReply(w, 201, u)
		return
	}
	pieces := strings.Split(path, "/")
	if len(pieces) < 2 {
		jsonReply(w, 404, map[string]string{"error": "Загрузка не найдена"})
		return
	}
	id := pieces[1]
	if len(pieces) == 2 {
		switch r.Method {
		case "GET":
			u, e := s.Store.Upload(id)
			if e != nil {
				apiError(w, e)
				return
			}
			jsonReply(w, 200, u)
			return
		case "DELETE":
			if e := s.Store.AbortUpload(id); e != nil {
				apiError(w, e)
				return
			}
			w.WriteHeader(204)
			return
		}
	}
	if len(pieces) == 3 && pieces[2] == "parts" && r.Method == "GET" {
		parts, e := s.Store.Parts(id)
		if e != nil {
			apiError(w, e)
			return
		}
		jsonReply(w, 200, parts)
		return
	}
	if len(pieces) == 3 && pieces[2] == "complete" && r.Method == "POST" {
		u, e := s.Store.Upload(id)
		if e != nil {
			apiError(w, e)
			return
		}
		if u.Mode != "resumable" && u.Mode != "parallel" {
			apiError(w, store.ErrUpload)
			return
		}
		o, e := s.Store.CompleteUpload(id, nil)
		if e != nil {
			apiError(w, e)
			return
		}
		jsonReply(w, 200, o)
		return
	}
	if len(pieces) == 4 && pieces[2] == "parts" && r.Method == "PUT" {
		n, e := strconv.Atoi(pieces[3])
		if e != nil {
			apiError(w, store.ErrPart)
			return
		}
		u, e := s.Store.Upload(id)
		if e != nil {
			apiError(w, e)
			return
		}
		if u.Mode != "resumable" && u.Mode != "parallel" {
			apiError(w, store.ErrUpload)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, store.ChunkSize+1)
		p, e := s.Store.UploadPart(r.Context(), id, n, r.Body, r.Header.Get("X-Content-SHA256"))
		if e != nil {
			apiError(w, e)
			return
		}
		jsonReply(w, 200, p)
		return
	}
	jsonReply(w, 405, map[string]string{"error": "Метод не поддерживается"})
}
func (s *Server) multipart(w http.ResponseWriter, r *http.Request, bucket, key, payload string) {
	q := r.URL.Query()
	id := q.Get("uploadId")
	fail := func(e error) {
		code := "InvalidPart"
		status := errorStatus(e)
		if e == store.ErrPrecondition {
			code = "PreconditionFailed"
		}
		if e == store.ErrDigest {
			code = "BadDigest"
		}
		if e == store.ErrUpload {
			code = "NoSuchUpload"
		}
		s3Error(w, status, code, e.Error())
	}
	readSmall := func() ([]byte, bool) {
		b, e := readS3Small(r, 2<<20)
		if e != nil {
			s3Failure(w, e)
			return nil, false
		}
		return b, true
	}

	if q.Has("uploads") && key != "" && r.Method == "POST" {
		if _, ok := readSmall(); !ok {
			return
		}
		u, e := s.Store.CreateUpload(bucket, key, r.Header.Get("Content-Type"), "s3", -1, requestOptions(r))
		if e != nil {
			fail(e)
			return
		}
		xmlReply(w, 200, struct {
			XMLName               xml.Name `xml:"InitiateMultipartUploadResult"`
			XMLNS                 string   `xml:"xmlns,attr"`
			Bucket, Key, UploadId string
		}{XMLNS: namespace, Bucket: bucket, Key: key, UploadId: u.ID})
		return
	}
	if q.Has("uploads") && key == "" && r.Method == "GET" {
		s.listUploads(w, r, bucket)
		return
	}
	u, e := s.Store.Upload(id)
	if e != nil {
		fail(e)
		return
	}
	if u.Bucket != bucket || u.Key != key || u.Mode != "s3" {
		fail(store.ErrUpload)
		return
	}
	switch r.Method {
	case "PUT":
		n, e := strconv.Atoi(q.Get("partNumber"))
		if e != nil {
			fail(store.ErrPart)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, store.MaxS3PartSize+1)
		body := newPayload(r)
		p, e := s.Store.UploadPart(r.Context(), id, n, body, payload, body.Verify)
		if e != nil {
			fail(e)
			return
		}
		w.Header().Set("ETag", `"`+p.ETag+`"`)
		w.WriteHeader(200)
	case "POST":
		raw, ok := readSmall()
		if !ok {
			return
		}
		var body struct {
			Parts []store.CompletedPart `xml:"Part"`
		}
		if e := xml.Unmarshal(raw, &body); e != nil {
			fail(store.ErrPart)
			return
		}
		o, e := s.Store.CompleteUpload(id, body.Parts, requestOptions(r))
		if e != nil {
			fail(e)
			return
		}
		xmlReply(w, 200, struct {
			XMLName                     xml.Name `xml:"CompleteMultipartUploadResult"`
			XMLNS                       string   `xml:"xmlns,attr"`
			Location, Bucket, Key, ETag string
		}{XMLNS: namespace, Location: s.S3Endpoint + "/" + bucket + "/" + key, Bucket: bucket, Key: key, ETag: `"` + o.ETag + `"`})
	case "DELETE":
		if _, ok := readSmall(); !ok {
			return
		}
		if e := s.Store.AbortUpload(id); e != nil {
			fail(e)
			return
		}
		w.WriteHeader(204)
	case "GET":
		s.listParts(w, r, u)
	default:
		s3Error(w, 405, "MethodNotAllowed", "Method not allowed")
	}
}
func (s *Server) listParts(w http.ResponseWriter, r *http.Request, u store.Upload) {
	q := r.URL.Query()
	marker, _ := strconv.Atoi(q.Get("part-number-marker"))
	limit := 1000
	var e error
	if q.Has("max-parts") {
		limit, e = strconv.Atoi(q.Get("max-parts"))
	}
	if e != nil || limit < 0 || limit > 1000 || marker < 0 {
		s3Error(w, 400, "InvalidArgument", "Invalid part pagination")
		return
	}
	parts, e := s.Store.Parts(u.ID)
	if e != nil {
		s3Error(w, 404, "NoSuchUpload", e.Error())
		return
	}
	type part struct {
		PartNumber   int
		LastModified string
		ETag         string
		Size         int64
	}
	out := struct {
		XMLName                                          xml.Name `xml:"ListPartsResult"`
		XMLNS                                            string   `xml:"xmlns,attr"`
		Bucket, Key, UploadId, StorageClass              string
		PartNumberMarker, NextPartNumberMarker, MaxParts int
		IsTruncated                                      bool
		Parts                                            []part `xml:"Part"`
	}{XMLNS: namespace, Bucket: u.Bucket, Key: u.Key, UploadId: u.ID, StorageClass: "STANDARD", PartNumberMarker: marker, MaxParts: limit}
	for _, p := range parts {
		if p.Number <= marker {
			continue
		}
		if len(out.Parts) == limit {
			out.IsTruncated = limit > 0
			break
		}
		out.Parts = append(out.Parts, part{p.Number, p.Modified.Format(time.RFC3339Nano), `"` + p.ETag + `"`, p.Size})
		out.NextPartNumberMarker = p.Number
	}
	xmlReply(w, 200, out)
}
func (s *Server) listUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, e := s.Store.Bucket(bucket); e != nil {
		s3Error(w, 404, "NoSuchBucket", e.Error())
		return
	}
	q := r.URL.Query()
	if q.Get("delimiter") != "" {
		s3Error(w, 501, "NotImplemented", "Delimiter is not supported for multipart upload listing")
		return
	}
	limit := 1000
	var e error
	if q.Has("max-uploads") {
		limit, e = strconv.Atoi(q.Get("max-uploads"))
	}
	if e != nil || limit < 0 || limit > 1000 {
		s3Error(w, 400, "InvalidArgument", "Invalid max-uploads")
		return
	}
	uploads, e := s.Store.Uploads(bucket)
	if e != nil {
		s3Error(w, 500, "InternalError", e.Error())
		return
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key == uploads[j].Key {
			return uploads[i].ID < uploads[j].ID
		}
		return uploads[i].Key < uploads[j].Key
	})
	type item struct{ Key, UploadId, Initiated, StorageClass string }
	out := struct {
		XMLName                                                                      xml.Name `xml:"ListMultipartUploadsResult"`
		XMLNS                                                                        string   `xml:"xmlns,attr"`
		Bucket, Prefix, KeyMarker, UploadIdMarker, NextKeyMarker, NextUploadIdMarker string
		MaxUploads                                                                   int
		IsTruncated                                                                  bool
		Uploads                                                                      []item `xml:"Upload"`
	}{XMLNS: namespace, Bucket: bucket, Prefix: q.Get("prefix"), KeyMarker: q.Get("key-marker"), UploadIdMarker: q.Get("upload-id-marker"), MaxUploads: limit}
	for _, u := range uploads {
		if u.Mode != "s3" || !strings.HasPrefix(u.Key, out.Prefix) || u.Key < out.KeyMarker || u.Key == out.KeyMarker && (out.UploadIdMarker == "" || u.ID <= out.UploadIdMarker) {
			continue
		}
		if len(out.Uploads) == limit {
			out.IsTruncated = limit > 0
			break
		}
		out.Uploads = append(out.Uploads, item{u.Key, u.ID, u.Created.Format(time.RFC3339Nano), "STANDARD"})
		out.NextKeyMarker, out.NextUploadIdMarker = u.Key, u.ID
	}
	xmlReply(w, 200, out)
}

package server

import (
	"encoding/xml"
	"maxstore/internal/store"
	"net/http"
	"net/url"
	"sort"
	"unicode/utf8"
)

type tag struct{ Key, Value string }
type taggingDocument struct {
	XMLName xml.Name `xml:"Tagging"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Tags    []tag    `xml:"TagSet>Tag"`
}

func validTags(tags []tag) error {
	if len(tags) > 10 {
		return store.ErrInvalid
	}
	seen := map[string]bool{}
	for _, t := range tags {
		if !utf8.ValidString(t.Key) || !utf8.ValidString(t.Value) || utf8.RuneCountInString(t.Key) < 1 || utf8.RuneCountInString(t.Key) > 128 || utf8.RuneCountInString(t.Value) > 256 || seen[t.Key] {
			return store.ErrInvalid
		}
		seen[t.Key] = true
	}
	return nil
}
func validateTagHeader(value string) error {
	q, e := url.ParseQuery(value)
	if e != nil {
		return store.ErrInvalid
	}
	tags := []tag{}
	for k, v := range q {
		if len(v) != 1 {
			return store.ErrInvalid
		}
		tags = append(tags, tag{k, v[0]})
	}
	return validTags(tags)
}
func (s *Server) tagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var tags map[string]string
	if key == "" {
		b, e := s.Store.Bucket(bucket)
		if e != nil {
			s3Error(w, 404, "NoSuchBucket", e.Error())
			return
		}
		tags = b.Tags
	} else {
		o, e := s.Store.Head(bucket, key)
		if e != nil {
			s3Failure(w, e)
			return
		}
		tags = o.Tags
	}
	switch r.Method {
	case "GET":
		out := taggingDocument{XMLNS: namespace, Tags: []tag{}}
		for k, v := range tags {
			out.Tags = append(out.Tags, tag{k, v})
		}
		sort.Slice(out.Tags, func(i, j int) bool { return out.Tags[i].Key < out.Tags[j].Key })
		xmlReply(w, 200, out)
	case "PUT":
		raw, e := readS3Small(r, 64<<10)
		if e != nil {
			s3Failure(w, e)
			return
		}
		var doc taggingDocument
		if e = xml.Unmarshal(raw, &doc); e != nil {
			s3Error(w, 400, "MalformedXML", e.Error())
			return
		}
		if e = validTags(doc.Tags); e != nil {
			s3Failure(w, e)
			return
		}
		tags = map[string]string{}
		for _, t := range doc.Tags {
			tags[t.Key] = t.Value
		}
		if e = s.Store.SetTags(bucket, key, tags); e != nil {
			s3Failure(w, e)
			return
		}
		w.WriteHeader(200)
	case "DELETE":
		if _, e := readS3Small(r, 4096); e != nil {
			s3Failure(w, e)
			return
		}
		if e := s.Store.SetTags(bucket, key, nil); e != nil {
			s3Failure(w, e)
			return
		}
		w.WriteHeader(204)
	default:
		s3Error(w, 405, "MethodNotAllowed", "Unsupported tagging method")
	}
}

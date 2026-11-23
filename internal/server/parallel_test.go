package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"maxstore/internal/provider"
)

type countedDownload struct {
	provider.Provider
	gets atomic.Int64
}

func (p *countedDownload) Get(ctx context.Context, ref provider.Ref) ([]byte, error) {
	p.gets.Add(1)
	return p.Provider.Get(ctx, ref)
}
func TestHTTPRangeDoesNotPrefetchOutsideRange(t *testing.T) {
	a := testApp(t)
	p := &countedDownload{Provider: a.Store.Providers["local"]}
	a.Store.Providers["local"] = p
	data := bytes.Repeat([]byte("test"), 6<<20)
	if _, e := a.Store.Put(context.Background(), "range-files", "file", "", bytes.NewReader(data), ""); e == nil {
		t.Fatal("put into missing bucket accepted")
	}
	if e := a.Store.CreateBucket("range-files", "local"); e != nil {
		t.Fatal(e)
	}
	if _, e := a.Store.Put(context.Background(), "range-files", "file", "", bytes.NewReader(data), ""); e != nil {
		t.Fatal(e)
	}
	r := signed(t, "GET", "/range-files/file", "")
	r.Header.Set("Range", "bytes=0-3")
	w := httptest.NewRecorder()
	a.S3().ServeHTTP(w, r)
	if w.Code != 206 || w.Body.String() != "test" || p.gets.Load() != 1 {
		t.Fatalf("range overfetched: status=%d gets=%d", w.Code, p.gets.Load())
	}
}
func TestParallelUploadAPIAndSparseProgress(t *testing.T) {
	a := testApp(t)
	h := a.UI()
	request := func(method, path string, body io.Reader, want int) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, body)
		r.Header.Set("Authorization", "Bearer "+a.Token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w
	}
	request(http.MethodPost, "/api/buckets", strings.NewReader(`{"name":"my-files","provider":"local"}`), 201)
	w := request("POST", "/api/uploads", strings.NewReader(`{"bucket":"my-files","key":"parallel","size":8388609,"parallel":true}`), 201)
	var u struct{ ID, Mode string }
	if e := json.Unmarshal(w.Body.Bytes(), &u); e != nil || u.Mode != "parallel" {
		t.Fatal("parallel mode not enabled", e)
	}
	request("PUT", "/api/uploads/"+u.ID+"/parts/2", strings.NewReader("z"), 200)
	w = request("GET", "/api/uploads/"+u.ID+"/parts", nil, 200)
	var parts []struct{ Number int }
	if e := json.Unmarshal(w.Body.Bytes(), &parts); e != nil || len(parts) != 1 || parts[0].Number != 2 {
		t.Fatal("sparse progress not exposed", e)
	}
	request("POST", "/api/uploads/"+u.ID+"/complete", nil, 400)
	request("PUT", "/api/uploads/"+u.ID+"/parts/1", bytes.NewReader(bytes.Repeat([]byte("a"), 8<<20)), 200)
	w = request("POST", "/api/uploads/"+u.ID+"/complete", nil, 200)
	var object struct {
		TreeSHA256 string `json:"tree_sha256"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &object); e != nil || object.TreeSHA256 == "" {
		t.Fatal("missing ordered checksum", e)
	}
	w = request("GET", "/api/objects?bucket=my-files&key=parallel", nil, 200)
	if w.Body.Len() != 8388609 || w.Body.Bytes()[8388608] != 'z' {
		t.Fatal("parallel upload served in wrong order")
	}
}

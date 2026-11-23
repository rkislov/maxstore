package server

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"maxstore/internal/provider"
	"maxstore/internal/store"
	"maxstore/web"
)

const access = "test-access"
const secret = "test-secret-at-least-16-characters"

func testApp(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	st, e := store.Open(dir, map[string]provider.Provider{"local": &provider.Local{Dir: filepath.Join(dir, "chunks")}}, 32<<20)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { st.Close() })
	return &Server{Store: st, Token: "console-token-123456", AccessKey: access, SecretKey: secret, DefaultProvider: "local", Assets: web.Assets}
}
func signed(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, "http://localhost:9000"+path, strings.NewReader(body))
	sum := hash(body)
	req.Header.Set("X-Amz-Content-Sha256", sum)
	e := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }).SignHTTP(context.Background(), aws.Credentials{AccessKeyID: access, SecretAccessKey: secret}, req, sum, "s3", "us-east-1", time.Now())
	if e != nil {
		t.Fatal(e)
	}
	return req
}
func call(t *testing.T, h http.Handler, method, path, body string, want int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, signed(t, method, path, body))
	if r.Code != want {
		t.Fatalf("%s %s: got %d want %d: %s", method, path, r.Code, want, r.Body.String())
	}
	return r
}
func TestS3SignedLifecycle(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	call(t, h, "HEAD", "/my-bucket", "", 200)
	call(t, h, "PUT", "/my-bucket/folder/a%20b.txt", "hello world", 200)
	res := call(t, h, "GET", "/my-bucket/folder/a%20b.txt", "", 200)
	if res.Body.String() != "hello world" {
		t.Fatal("body mismatch")
	}
	head := call(t, h, "HEAD", "/my-bucket/folder/a%20b.txt", "", 200)
	if head.Header().Get("Content-Length") != "11" {
		t.Fatal("head size mismatch")
	}
	req := signed(t, "GET", "/my-bucket/folder/a%20b.txt", "")
	req.Header.Set("Range", "bytes=1-4")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 206 || rr.Body.String() != "ello" {
		t.Fatalf("range: %d %s", rr.Code, rr.Body.String())
	}
	call(t, h, "DELETE", "/my-bucket", "", 409)
	call(t, h, "DELETE", "/my-bucket/folder/a%20b.txt", "", 204)
	call(t, h, "DELETE", "/my-bucket", "", 204)
	call(t, h, "HEAD", "/my-bucket", "", 404)
}
func TestSignatureRejectsTampering(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	for _, tamper := range []func(*http.Request){func(r *http.Request) { r.URL.Path = "/changed" }, func(r *http.Request) { r.Host = "evil.example" }, func(r *http.Request) { r.Header.Set("X-Amz-Date", "20200101T000000Z") }, func(r *http.Request) { r.Header.Del("Authorization") }} {
		req := signed(t, "GET", "/", "")
		tamper(req)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != 403 {
			t.Fatalf("accepted tampered request: %d", rr.Code)
		}
	}
	call(t, h, "PUT", "/my-bucket", "", 200)
	r := signed(t, "PUT", "/my-bucket/file", "original")
	r.Body = io.NopCloser(strings.NewReader("tampered"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code < 400 {
		t.Fatal("accepted tampered payload")
	}
	if _, e := app.Store.Head("my-bucket", "file"); e == nil {
		t.Fatal("tampered payload published")
	}
}
func TestS3PaginationAndDelimiter(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	for _, key := range []string{"a.txt", "b.txt", "folder/a.txt", "folder/b.txt", "файл.txt"} {
		call(t, h, "PUT", "/my-bucket/"+key, "data", 200)
	}
	res := call(t, h, "GET", "/my-bucket?list-type=2&max-keys=2&delimiter=%2F", "", 200)
	var list struct {
		IsTruncated           bool
		NextContinuationToken string
		KeyCount              int
		Contents              []struct{ Key string }
		CommonPrefixes        []struct{ Prefix string }
	}
	if e := xml.Unmarshal(res.Body.Bytes(), &list); e != nil {
		t.Fatal(e)
	}
	if !list.IsTruncated || list.KeyCount != 2 || len(list.Contents) != 2 {
		t.Fatalf("bad page: %+v", list)
	}
	res = call(t, h, "GET", "/my-bucket?list-type=2&max-keys=2&delimiter=%2F&continuation-token="+list.NextContinuationToken, "", 200)
	list = struct {
		IsTruncated           bool
		NextContinuationToken string
		KeyCount              int
		Contents              []struct{ Key string }
		CommonPrefixes        []struct{ Prefix string }
	}{}
	if e := xml.Unmarshal(res.Body.Bytes(), &list); e != nil {
		t.Fatal(e)
	}
	if list.IsTruncated || len(list.CommonPrefixes) != 1 || list.CommonPrefixes[0].Prefix != "folder/" {
		t.Fatalf("bad second page: %+v", list)
	}
	call(t, h, "POST", "/my-bucket?acl", "", 501)
}
func TestSDKPresignedURL(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	call(t, h, "PUT", "/my-bucket/file", "presigned", 200)
	req, _ := http.NewRequest("GET", "http://localhost:9000/my-bucket/file?X-Amz-Expires=60", nil)
	u, _, e := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }).PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: access, SecretAccessKey: secret}, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now())
	if e != nil {
		t.Fatal(e)
	}
	r := httptest.NewRequest("GET", u, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code != 200 || rr.Body.String() != "presigned" {
		t.Fatalf("presign rejected: %d %s", rr.Code, rr.Body.String())
	}
}
func TestConsoleAuthAndUpload(t *testing.T) {
	app := testApp(t)
	h := app.UI()
	r := httptest.NewRequest("GET", "/api/status", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code != 401 {
		t.Fatal("console API is public")
	}
	run := func(method, path, body string, want int) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+app.Token)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		if rr.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, rr.Code, rr.Body.String())
		}
		return rr
	}
	run("POST", "/api/buckets", `{"name":"my-bucket","provider":"local"}`, 201)
	run("PUT", "/api/objects?bucket=my-bucket&key=test.txt", "console", 201)
	res := run("GET", "/api/objects?bucket=my-bucket&key=test.txt", "", 200)
	if res.Body.String() != "console" {
		t.Fatal("console round trip failed")
	}
	run("DELETE", "/api/objects?bucket=my-bucket&key=test.txt", "", 204)
	run("DELETE", "/api/buckets/my-bucket", "", 204)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
	if rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte("MaxStore")) {
		t.Fatal("embedded UI missing")
	}
	if rr.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("missing CSP")
	}
}

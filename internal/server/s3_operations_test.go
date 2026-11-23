package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func headerCall(t *testing.T, h http.Handler, method, path, body string, headers map[string]string, want int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "http://localhost:9000"+path, strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	sum := hash(body)
	r.Header.Set("X-Amz-Content-Sha256", sum)
	if e := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }).SignHTTP(context.Background(), aws.Credentials{AccessKeyID: access, SecretAccessKey: secret}, r, sum, "s3", "us-east-1", time.Now()); e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s got %d want %d: %s", method, path, w.Code, want, w.Body.String())
	}
	return w
}
func TestS3MetadataCopyConditionalAndDelete(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	original := headerCall(t, h, "PUT", "/my-bucket/a", "hello", map[string]string{"X-Amz-Meta-Owner": "Roman", "Cache-Control": "max-age=60", "Content-Type": "text/plain", "If-None-Match": "*"}, 200)
	headerCall(t, h, "PUT", "/my-bucket/a", "clobber", map[string]string{"If-None-Match": "*"}, 412)
	head := call(t, h, "HEAD", "/my-bucket/a", "", 200)
	if head.Header().Get("X-Amz-Meta-Owner") != "Roman" || head.Header().Get("Cache-Control") != "max-age=60" {
		t.Fatal("metadata lost")
	}
	headerCall(t, h, "PUT", "/my-bucket/b", "", map[string]string{"X-Amz-Copy-Source": "/my-bucket/a", "X-Amz-Copy-Source-If-Match": original.Header().Get("ETag")}, 200)
	if got := call(t, h, "GET", "/my-bucket/b", "", 200); got.Body.String() != "hello" || got.Header().Get("X-Amz-Meta-Owner") != "Roman" {
		t.Fatal("copy lost content or metadata")
	}
	headerCall(t, h, "PUT", "/my-bucket/c", "", map[string]string{"X-Amz-Copy-Source": "/my-bucket/a", "X-Amz-Copy-Source-If-Match": "\"bad\""}, 412)
	headerCall(t, h, "PUT", "/my-bucket/c", "", map[string]string{"X-Amz-Copy-Source": "/my-bucket/a", "X-Amz-Metadata-Directive": "REPLACE", "X-Amz-Meta-Owner": "replacement"}, 200)
	if head := call(t, h, "HEAD", "/my-bucket/c", "", 200); head.Header().Get("X-Amz-Meta-Owner") != "replacement" {
		t.Fatal("REPLACE ignored")
	}
	result := call(t, h, "POST", "/my-bucket?delete", `<Delete><Object><Key>a</Key></Object><Object><Key>b</Key></Object><Object><Key>missing</Key></Object></Delete>`, 200)
	var out struct{ Deleted []struct{ Key string } }
	if e := xml.Unmarshal(result.Body.Bytes(), &out); e != nil || len(out.Deleted) != 3 {
		t.Fatalf("bulk delete: %s", result.Body.String())
	}
	call(t, h, "GET", "/my-bucket/a", "", 404)
}
func TestS3ChecksumRejectedBeforePublication(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	sum := sha256.Sum256([]byte("hello"))
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	headerCall(t, h, "PUT", "/my-bucket/a", "hello", map[string]string{"X-Amz-Checksum-Sha256": checksum}, 200)
	headerCall(t, h, "PUT", "/my-bucket/a", "wrong", map[string]string{"X-Amz-Checksum-Sha256": checksum}, 400)
	got := call(t, h, "GET", "/my-bucket/a", "", 200)
	if got.Body.String() != "hello" || got.Header().Get("X-Amz-Checksum-Sha256") != checksum {
		t.Fatal("bad overwrite changed published object")
	}
}
func TestS3MultipartCopyRange(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	call(t, h, "PUT", "/my-bucket/source", "hello world", 200)
	init := headerCall(t, h, "POST", "/my-bucket/dest?uploads", "", map[string]string{"X-Amz-Meta-Owner": "Roman"}, 200)
	var upload struct{ UploadId string }
	xml.Unmarshal(init.Body.Bytes(), &upload)
	result := headerCall(t, h, "PUT", "/my-bucket/dest?uploadId="+upload.UploadId+"&partNumber=1", "", map[string]string{"X-Amz-Copy-Source": "/my-bucket/source", "X-Amz-Copy-Source-Range": "bytes=6-10"}, 200)
	var part struct{ ETag string }
	xml.Unmarshal(result.Body.Bytes(), &part)
	call(t, h, "POST", "/my-bucket/dest?uploadId="+upload.UploadId, `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>`+part.ETag+`</ETag></Part></CompleteMultipartUpload>`, 200)
	got := call(t, h, "GET", "/my-bucket/dest", "", 200)
	if got.Body.String() != "world" || got.Header().Get("X-Amz-Meta-Owner") != "Roman" {
		t.Fatal("multipart copy lost data/metadata")
	}
}

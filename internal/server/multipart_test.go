package server

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestS3MultipartLifecycle(t *testing.T) {
	app := testApp(t)
	h := app.S3()
	call(t, h, "PUT", "/my-bucket", "", 200)
	create := call(t, h, "POST", "/my-bucket/large?uploads", "", 200)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	if e := xml.Unmarshal(create.Body.Bytes(), &initiated); e != nil || initiated.UploadID == "" {
		t.Fatal("create multipart failed", e)
	}
	path := "/my-bucket/large?uploadId=" + initiated.UploadID
	call(t, h, "PUT", "/another/key?uploadId="+initiated.UploadID+"&partNumber=1", "not allowed", 404)
	call(t, h, "PUT", path+"&partNumber=10001", "bad", 400)
	first := strings.Repeat("a", 5<<20)
	one := call(t, h, "PUT", path+"&partNumber=1", first, 200)
	two := call(t, h, "PUT", path+"&partNumber=2", "tail", 200)
	listed := call(t, h, "GET", path+"&max-parts=1", "", 200)
	var parts struct {
		IsTruncated          bool
		NextPartNumberMarker int
		Parts                []struct {
			Number int `xml:"PartNumber"`
		} `xml:"Part"`
	}
	if e := xml.Unmarshal(listed.Body.Bytes(), &parts); e != nil || !parts.IsTruncated || parts.NextPartNumberMarker != 1 {
		t.Fatal("invalid list parts", e, listed.Body.String())
	}
	call(t, h, "GET", path+"&part-number-marker=1", "", 200)
	uploads := call(t, h, "GET", "/my-bucket?uploads", "", 200)
	if !strings.Contains(uploads.Body.String(), initiated.UploadID) {
		t.Fatal("pending upload missing")
	}
	call(t, h, "POST", path, `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>wrong</ETag></Part></CompleteMultipartUpload>`, 400)
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, one.Header().Get("ETag"), two.Header().Get("ETag"))
	call(t, h, "POST", path, complete, 200)
	call(t, h, "POST", path, complete, 200)
	got := call(t, h, "GET", "/my-bucket/large", "", 200)
	if !bytes.Equal(got.Body.Bytes(), []byte(first+"tail")) {
		t.Fatal("multipart object mismatch")
	}
	req := signed(t, "GET", "/my-bucket/large", "")
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", len(first)-2, len(first)+3))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 206 || w.Body.String() != "aatail" {
		t.Fatal("multipart boundary range failed", w.Code, w.Body.String())
	}
	create = call(t, h, "POST", "/my-bucket/abort?uploads", "", 200)
	if e := xml.Unmarshal(create.Body.Bytes(), &initiated); e != nil {
		t.Fatal(e)
	}
	path = "/my-bucket/abort?uploadId=" + initiated.UploadID
	call(t, h, "PUT", path+"&partNumber=1", "data", 200)
	call(t, h, "DELETE", path, "", 204)
	call(t, h, "GET", path, "", 404)
}

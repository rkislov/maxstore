package server

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"net/http/httptest"
	"testing"
)

func TestOfficialSDKLifecycle(t *testing.T) {
	app := testApp(t)
	srv := httptest.NewServer(app.S3())
	defer srv.Close()
	ctx := context.Background()
	c := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(access, secret, "")})
	bucket := aws.String("sdk-bucket")
	key := aws.String("folder/тест.txt")
	if _, e := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); e != nil {
		t.Fatal(e)
	}
	data := bytes.Repeat([]byte("SDK transfer\n"), 1024)
	if _, e := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: bytes.NewReader(data), ContentType: aws.String("text/plain"), Metadata: map[string]string{"owner": "Roman"}, Tagging: aws.String("project=maxstore")}); e != nil {
		t.Fatal(e)
	}
	got, e := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
	if e != nil {
		t.Fatal(e)
	}
	raw, e := io.ReadAll(got.Body)
	got.Body.Close()
	if e != nil || !bytes.Equal(raw, data) {
		t.Fatalf("SDK download mismatch: %v", e)
	}
	head, e := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
	if e != nil || head.Metadata["owner"] != "Roman" {
		t.Fatalf("metadata lost: %v", e)
	}
	tags, e := c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: bucket, Key: key})
	if e != nil || len(tags.TagSet) != 1 {
		t.Fatalf("tags lost: %v", e)
	}
	if _, e = c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: bucket, Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("environment"), Value: aws.String("test")}}}}); e != nil {
		t.Fatal(e)
	}
	copied := aws.String("copy.txt")
	if _, e = c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: bucket, Key: copied, CopySource: aws.String("sdk-bucket/folder/%D1%82%D0%B5%D1%81%D1%82.txt")}); e != nil {
		t.Fatal(e)
	}
	list, e := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket})
	if e != nil || len(list.Contents) != 2 {
		t.Fatalf("list: %v", e)
	}
	if _, e = c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: key}, {Key: copied}}}}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}); e != nil {
		t.Fatal(e)
	}
}

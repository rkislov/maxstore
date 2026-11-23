package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"maxstore/internal/provider"
)

func TestResumeAcrossRestartAndIdempotentPart(t *testing.T) {
	s, dir := openTest(t)
	s.MaxSize = 0
	ctx := context.Background()
	first := bytes.Repeat([]byte{7}, ChunkSize)
	last := []byte("last bytes")
	u, e := s.CreateUpload("test-bucket", "resume", "application/octet-stream", "resumable", int64(len(first)+len(last)))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.UploadPart(ctx, u.ID, 2, bytes.NewReader(last), ""); !errors.Is(e, ErrPart) {
		t.Fatal("out-of-order part accepted", e)
	}
	one, e := s.UploadPart(ctx, u.ID, 1, bytes.NewReader(first), "")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Head("test-bucket", "resume"); !errors.Is(e, ErrNotFound) {
		t.Fatal("partial object visible")
	}
	if e = s.DeleteBucket("test-bucket"); !errors.Is(e, ErrConflict) {
		t.Fatal("bucket with pending upload deleted")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := Open(dir, s.Providers, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	status, e := next.Upload(u.ID)
	if e != nil || status.Offset != ChunkSize || status.NextPart != 2 {
		t.Fatalf("resume state lost: %+v %v", status, e)
	}
	replay, e := next.UploadPart(ctx, u.ID, 1, bytes.NewReader(first), "")
	if e != nil || replay != one {
		t.Fatal("part retry is not idempotent", e)
	}
	if _, e = next.UploadPart(ctx, u.ID, 1, bytes.NewReader(bytes.Repeat([]byte{8}, ChunkSize)), ""); !errors.Is(e, ErrPart) {
		t.Fatal("changed retry accepted")
	}
	if _, e = next.CompleteUpload(u.ID, nil); !errors.Is(e, ErrPart) {
		t.Fatal("incomplete upload published")
	}
	if _, e = next.UploadPart(ctx, u.ID, 2, bytes.NewReader(last), ""); e != nil {
		t.Fatal(e)
	}
	o, e := next.CompleteUpload(u.ID, nil)
	if e != nil {
		t.Fatal(e)
	}
	again, e := next.CompleteUpload(u.ID, nil)
	if e != nil || again.ID != o.ID {
		t.Fatal("completion retry failed")
	}
	expected := append(first, last...)
	sum := sha256.Sum256(expected)
	if o.SHA256 != hex.EncodeToString(sum[:]) || !bytes.Equal(readTest(t, next, "resume"), expected) {
		t.Fatal("resumed object checksum/body mismatch")
	}
}
func TestPartFailureAbortAndExpiry(t *testing.T) {
	s, dir := openTest(t)
	ctx := context.Background()
	u, e := s.CreateUpload("test-bucket", "file", "", "resumable", 3)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.UploadPart(ctx, u.ID, 1, bytes.NewBufferString("abc"), "wrong"); !errors.Is(e, ErrPart) {
		t.Fatal("bad checksum accepted")
	}
	status, _ := s.Upload(u.ID)
	if status.Offset != 0 {
		t.Fatal("failed part advanced progress")
	}
	if _, e = s.UploadPart(ctx, u.ID, 1, bytes.NewBufferString("abc"), ""); e != nil {
		t.Fatal(e)
	}
	if e = s.AbortUpload(u.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "chunks", "*"))
	if len(files) != 0 {
		t.Fatal("abort leaked chunks")
	}
	u, e = s.CreateUpload("test-bucket", "expire", "", "resumable", 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.UploadPart(ctx, u.ID, 1, bytes.NewBufferString("x"), ""); e != nil {
		t.Fatal(e)
	}
	if e = s.ExpireUploads(time.Now().Add(8 * 24 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Upload(u.ID); !errors.Is(e, ErrUpload) {
		t.Fatal("stale upload retained")
	}
	if e = s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestMultipartPartOverwriteAndValidation(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()
	first := bytes.Repeat([]byte{'a'}, 5<<20)
	u, e := s.CreateUpload("test-bucket", "multi", "", "s3", -1)
	if e != nil {
		t.Fatal(e)
	}
	p1, e := s.UploadPart(ctx, u.ID, 2, bytes.NewReader(first), "")
	if e != nil {
		t.Fatal(e)
	}
	p2, e := s.UploadPart(ctx, u.ID, 7, bytes.NewBufferString("tail"), "")
	if e != nil {
		t.Fatal(e)
	}
	bad := []CompletedPart{{7, p2.ETag}, {2, p1.ETag}}
	if _, e = s.CompleteUpload(u.ID, bad); !errors.Is(e, ErrPart) {
		t.Fatal("unordered completion accepted")
	}
	if _, e = s.UploadPart(ctx, u.ID, 7, bytes.NewBufferString("new tail"), ""); e != nil {
		t.Fatal(e)
	}
	if _, e = s.CompleteUpload(u.ID, []CompletedPart{{2, p1.ETag}, {7, p2.ETag}}); !errors.Is(e, ErrPart) {
		t.Fatal("stale etag accepted")
	}
	parts, e := s.Parts(u.ID)
	if e != nil {
		t.Fatal(e)
	}
	selected := []CompletedPart{}
	for _, p := range parts {
		selected = append(selected, CompletedPart{p.Number, p.ETag})
	}
	o, e := s.CompleteUpload(u.ID, selected)
	if e != nil {
		t.Fatal(e)
	}
	if o.SHA256 != "" || len(o.ETag) != 34 || o.ETag[len(o.ETag)-2:] != "-2" {
		t.Fatal("invalid multipart etag", o.ETag)
	}
	if !bytes.Equal(readTest(t, s, "multi"), append(first, []byte("new tail")...)) {
		t.Fatal("multipart read failed")
	}
}

type counted struct {
	provider.Provider
	Gets atomic.Int64
}

func (p *counted) Get(ctx context.Context, ref provider.Ref) ([]byte, error) {
	p.Gets.Add(1)
	return p.Provider.Get(ctx, ref)
}
func TestReaderRangeAndSnapshotSurviveGC(t *testing.T) {
	s, dir := openTest(t)
	p := &counted{Provider: s.Providers["local"]}
	s.Providers["local"] = p
	ctx := context.Background()
	data := append(bytes.Repeat([]byte{'x'}, ChunkSize), []byte("tail")...)
	if _, e := s.Put(ctx, "test-bucket", "file", "", bytes.NewReader(data), ""); e != nil {
		t.Fatal(e)
	}
	r, _, e := s.OpenReader(ctx, "test-bucket", "file")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Seek(ChunkSize, io.SeekStart); e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(r)
	if e != nil || string(got) != "tail" || p.Gets.Load() != 1 {
		t.Fatalf("range fetched unrelated chunks: %d %q %v", p.Gets.Load(), got, e)
	}
	if _, e = s.Put(ctx, "test-bucket", "file", "", bytes.NewBufferString("replacement"), ""); e != nil {
		t.Fatal(e)
	}
	if e = s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
	if s.PendingGC() != 2 {
		t.Fatal("GC removed pinned snapshot")
	}
	if _, e = r.Seek(0, io.SeekStart); e != nil {
		t.Fatal(e)
	}
	got, e = io.ReadAll(r)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("reader snapshot changed", e)
	}
	r.Close()
	if e = s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
	if s.PendingGC() != 0 {
		t.Fatal("unpinned chunks not collected")
	}
	temps, _ := filepath.Glob(filepath.Join(dir, "download-*"))
	if len(temps) != 0 {
		t.Fatal("stream created plaintext temporary files")
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

type sink struct {
	mu   sync.Mutex
	Puts int
	Max  int
}

func (p *sink) Put(ctx context.Context, b []byte) (provider.Ref, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Puts++
	p.Max = max(p.Max, len(b))
	return provider.Ref{ID: provider.ID()}, nil
}
func (p *sink) Get(context.Context, provider.Ref) ([]byte, error) { return nil, os.ErrNotExist }
func (p *sink) Delete(context.Context, provider.Ref) error        { return nil }
func TestObjectBeyondOneGiBWithoutSpooling(t *testing.T) {
	if testing.Short() {
		t.Skip("1 GiB streaming regression")
	}
	dir := t.TempDir()
	p := &sink{}
	s, e := Open(dir, map[string]provider.Provider{"sink": p}, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.CreateBucket("big-files", "sink"); e != nil {
		t.Fatal(e)
	}
	const size = (1 << 30) + 123
	o, e := s.Put(context.Background(), "big-files", "large", "", io.LimitReader(zeros{}, size), "")
	if e != nil || o.Size != size || p.Puts != 129 || p.Max > ChunkSize+28 {
		t.Fatalf("large streaming write: size=%d puts=%d max=%d error=%v", o.Size, p.Puts, p.Max, e)
	}
}

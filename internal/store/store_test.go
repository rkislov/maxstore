package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"maxstore/internal/provider"
)

func openTest(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, e := Open(dir, map[string]provider.Provider{"local": &provider.Local{Dir: filepath.Join(dir, "chunks")}}, 32<<20)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	if e = s.CreateBucket("test-bucket", "local"); e != nil {
		t.Fatal(e)
	}
	return s, dir
}
func readTest(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	f, _, e := s.OpenReader(context.Background(), "test-bucket", key)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, e := io.ReadAll(f)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestEncryptedChunkRoundTrip(t *testing.T) {
	s, dir := openTest(t)
	data := bytes.Repeat([]byte("private-data"), ChunkSize/12+17)
	o, e := s.Put(context.Background(), "test-bucket", "nested/файл.txt", "text/plain", bytes.NewReader(data), "")
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.object("test-bucket", o.Key)
	if e != nil || len(r.Chunks) != 2 {
		t.Fatalf("expected two chunks, got %d, %v", len(r.Chunks), e)
	}
	raw, e := os.ReadFile(filepath.Join(dir, "chunks", r.Chunks[0].ID))
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(raw, []byte("private-data")) {
		t.Fatal("plaintext leaked")
	}
	if got := readTest(t, s, o.Key); !bytes.Equal(data, got) {
		t.Fatal("round trip mismatch")
	}
	if e = s.DeleteBucket("test-bucket"); !errors.Is(e, ErrConflict) {
		t.Fatal("nonempty bucket deleted")
	}
}
func TestCorruptChunkRejected(t *testing.T) {
	s, dir := openTest(t)
	_, e := s.Put(context.Background(), "test-bucket", "test", "", bytes.NewBufferString("secret"), "")
	if e != nil {
		t.Fatal(e)
	}
	r, _ := s.object("test-bucket", "test")
	p := filepath.Join(dir, "chunks", r.Chunks[0].ID)
	raw, _ := os.ReadFile(p)
	raw[len(raw)-1] ^= 1
	if e = os.WriteFile(p, raw, 0600); e != nil {
		t.Fatal(e)
	}
	reader, _, e := s.OpenReader(context.Background(), "test-bucket", "test")
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	if _, e = io.ReadAll(reader); e == nil {
		t.Fatal("corrupt data accepted")
	}
	temps, _ := filepath.Glob(filepath.Join(dir, "download-*"))
	if len(temps) != 0 {
		t.Fatal("failed download leaked temp files")
	}
}
func TestOverwriteRollbackAndGC(t *testing.T) {
	s, dir := openTest(t)
	ctx := context.Background()
	if _, e := s.Put(ctx, "test-bucket", "test", "", bytes.NewBufferString("old"), ""); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Put(ctx, "test-bucket", "test", "", bytes.NewBufferString("bad"), "wrong-checksum"); e == nil {
		t.Fatal("bad checksum accepted")
	}
	if string(readTest(t, s, "test")) != "old" {
		t.Fatal("failed write replaced existing data")
	}
	if s.PendingGC() != 1 {
		t.Fatal("failed upload not queued")
	}
	if _, e := s.Put(ctx, "test-bucket", "test", "", bytes.NewBufferString("new"), ""); e != nil {
		t.Fatal(e)
	}
	if e := s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "chunks", "*"))
	if len(files) != 1 {
		t.Fatalf("expected 1 live chunk, got %d", len(files))
	}
	if string(readTest(t, s, "test")) != "new" {
		t.Fatal("overwrite mismatch")
	}
	if e := s.Delete("test-bucket", "test"); e != nil {
		t.Fatal(e)
	}
	if e := s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
	if e := s.DeleteBucket("test-bucket"); e != nil {
		t.Fatal(e)
	}
}
func TestEmptyAndLimit(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()
	if _, e := s.Put(ctx, "test-bucket", "empty", "", bytes.NewReader(nil), ""); e != nil {
		t.Fatal(e)
	}
	if len(readTest(t, s, "empty")) != 0 {
		t.Fatal("empty mismatch")
	}
	s.MaxSize = 3
	if _, e := s.Put(ctx, "test-bucket", "large", "", bytes.NewBufferString("four"), ""); !errors.Is(e, ErrTooLarge) {
		t.Fatalf("limit not enforced: %v", e)
	}
	if _, e := s.Head("test-bucket", "large"); !errors.Is(e, ErrNotFound) {
		t.Fatal("failed object was published")
	}
	for _, key := range []string{"", "bad\x00key", "bad\nkey"} {
		if _, e := s.Put(ctx, "test-bucket", key, "", bytes.NewReader(nil), ""); !errors.Is(e, ErrInvalid) {
			t.Fatal("invalid key accepted")
		}
	}
}
func TestRestartPreservesKeysAndMetadata(t *testing.T) {
	s, dir := openTest(t)
	if _, e := s.Put(context.Background(), "test-bucket", "persist", "", bytes.NewBufferString("durable"), ""); e != nil {
		t.Fatal(e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := Open(dir, s.Providers, s.MaxSize)
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	if string(readTest(t, next, "persist")) != "durable" {
		t.Fatal("restart lost data")
	}
}

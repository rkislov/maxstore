package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"maxstore/internal/provider"
)

type overlap struct {
	active, peak, started atomic.Int64
	gate                  chan struct{}
	threshold             int64
	once                  sync.Once
}

func (o *overlap) enter(ctx context.Context) (func(), error) {
	n := o.active.Add(1)
	for previous := o.peak.Load(); n > previous; previous = o.peak.Load() {
		if o.peak.CompareAndSwap(previous, n) {
			break
		}
	}
	if o.started.Add(1) >= o.threshold {
		o.once.Do(func() { close(o.gate) })
	}
	select {
	case <-o.gate:
		return func() { o.active.Add(-1) }, nil
	case <-ctx.Done():
		o.active.Add(-1)
		return nil, ctx.Err()
	}
}

type parallelProvider struct {
	provider.Provider
	put, get *overlap
}

func (p *parallelProvider) Put(ctx context.Context, b []byte) (provider.Ref, error) {
	if p.put != nil {
		done, e := p.put.enter(ctx)
		if e != nil {
			return provider.Ref{}, e
		}
		defer done()
	}
	return p.Provider.Put(ctx, b)
}
func (p *parallelProvider) Get(ctx context.Context, ref provider.Ref) ([]byte, error) {
	if p.get != nil {
		done, e := p.get.enter(ctx)
		if e != nil {
			return nil, e
		}
		defer done()
	}
	return p.Provider.Get(ctx, ref)
}
func barrier(n int) *overlap { return &overlap{gate: make(chan struct{}), threshold: int64(n)} }
func TestPutAndReadActuallyOverlapFourProviderRequests(t *testing.T) {
	s, _ := openTest(t)
	p := &parallelProvider{Provider: s.Providers["local"], put: barrier(4), get: barrier(4)}
	s.Providers["local"] = p
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte("parallel"), 4*ChunkSize/8)
	o, e := s.Put(ctx, "test-bucket", "four-streams", "", bytes.NewReader(data), "")
	if e != nil {
		t.Fatal(e)
	}
	if p.put.peak.Load() != 4 {
		t.Fatal("upload was not bounded at four", p.put.peak.Load())
	}
	r, _, e := s.OpenReader(ctx, "test-bucket", "four-streams")
	if e != nil {
		t.Fatal(e)
	}
	h := sha256.New()
	n, e := io.Copy(h, r)
	r.Close()
	if e != nil || n != int64(len(data)) || hex.EncodeToString(h.Sum(nil)) != o.SHA256 {
		t.Fatal("parallel read reordered bytes", e)
	}
	if p.get.peak.Load() != 4 {
		t.Fatal("download was not bounded at four", p.get.peak.Load())
	}
	if len(s.downloadSlots) != 0 || len(s.uploadSlots) != 0 || len(s.pins) != 0 {
		t.Fatal("pipeline leaked slots/pins")
	}
}
func TestParallelResumeGapsAndTreeIntegrity(t *testing.T) {
	s, dir := openTest(t)
	ctx := context.Background()
	first := bytes.Repeat([]byte{'a'}, ChunkSize)
	second := bytes.Repeat([]byte{'b'}, ChunkSize)
	last := []byte("tail")
	u, e := s.CreateUpload("test-bucket", "unordered", "", "parallel", 2*ChunkSize+int64(len(last)))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.UploadPart(ctx, u.ID, 2, bytes.NewReader(second), ""); e != nil {
		t.Fatal(e)
	}
	state, _ := s.Upload(u.ID)
	if state.Offset != ChunkSize || state.NextPart != 1 {
		t.Fatalf("gap not preserved %+v", state)
	}
	if _, e = s.CompleteUpload(u.ID, nil); !errors.Is(e, ErrPart) {
		t.Fatal("published with gap")
	}
	s.Close()
	s, e = Open(dir, s.Providers, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	parts, e := s.Parts(u.ID)
	if e != nil || len(parts) != 1 || parts[0].Number != 2 {
		t.Fatal("sparse progress lost after restart", e)
	}
	if _, e = s.UploadPart(ctx, u.ID, 3, bytes.NewReader(last), ""); e != nil {
		t.Fatal(e)
	}
	if _, e = s.UploadPart(ctx, u.ID, 1, bytes.NewReader(first), ""); e != nil {
		t.Fatal(e)
	}
	o, e := s.CompleteUpload(u.ID, nil)
	if e != nil {
		t.Fatal(e)
	}
	if o.TreeSHA256 == "" || o.SHA256 != "" {
		t.Fatal("missing/unlabelled parallel integrity digest")
	}
	want := append(append(first, second...), last...)
	if !bytes.Equal(readTest(t, s, "unordered"), want) {
		t.Fatal("out-of-order parts assembled incorrectly")
	}
	rec, e := s.object("test-bucket", "unordered")
	if e != nil {
		t.Fatal(e)
	}
	rec.Object.TreeSHA256 = "wrong"
	s.DB.Update(func(tx *bolt.Tx) error { return putJSON(tx.Bucket([]byte("objects")), "test-bucket\x00unordered", rec) })
	r, _, e := s.OpenReader(ctx, "test-bucket", "unordered")
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if _, e = io.Copy(io.Discard, r); e == nil {
		t.Fatal("invalid tree checksum accepted")
	}
}
func TestConcurrentS3CommitsDoNotLoseOffsets(t *testing.T) {
	s, _ := openTest(t)
	p := &parallelProvider{Provider: s.Providers["local"], put: barrier(4)}
	s.Providers["local"] = p
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, e := s.CreateUpload("test-bucket", "s3-concurrent", "", "s3", -1)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for n := 1; n <= 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, e := s.UploadPart(ctx, u.ID, n, bytes.NewBufferString("data"), "")
			errs <- e
		}(n)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	state, _ := s.Upload(u.ID)
	parts, _ := s.Parts(u.ID)
	if state.Offset != 16 || len(parts) != 4 || p.put.peak.Load() != 4 {
		t.Fatalf("lost concurrent commits: %+v parts=%d peak=%d", state, len(parts), p.put.peak.Load())
	}
}
func TestConcurrentLimitAndAbortInFlight(t *testing.T) {
	s, _ := openTest(t)
	if e := s.ConfigureConcurrency(2, 2); e != nil {
		t.Fatal(e)
	}
	s.MaxSize = 6
	p := &parallelProvider{Provider: s.Providers["local"], put: barrier(2)}
	s.Providers["local"] = p
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, e := s.CreateUpload("test-bucket", "limited", "", "s3", -1)
	if e != nil {
		t.Fatal(e)
	}
	errs := make(chan error, 2)
	for n := 1; n <= 2; n++ {
		go func(n int) { _, e := s.UploadPart(ctx, u.ID, n, bytes.NewBufferString("data"), ""); errs <- e }(n)
	}
	failed := 0
	for i := 0; i < 2; i++ {
		e := <-errs
		if errors.Is(e, ErrTooLarge) {
			failed++
		} else if e != nil {
			t.Fatal(e)
		}
	}
	state, _ := s.Upload(u.ID)
	if failed != 1 || state.Offset != 4 {
		t.Fatal("concurrent size limit bypassed")
	}
	if e = s.AbortUpload(u.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
	if s.PendingGC() != 0 {
		t.Fatal("failed part cleanup incomplete")
	}
	// Abort must not wait on a slow provider. A finishing part cannot resurrect it.
	p.put = barrier(2)
	u, e = s.CreateUpload("test-bucket", "aborted", "", "s3", -1)
	if e != nil {
		t.Fatal(e)
	}
	go func() { _, e := s.UploadPart(ctx, u.ID, 1, bytes.NewBufferString("data"), ""); errs <- e }()
	for p.put.started.Load() == 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if e = s.AbortUpload(u.ID); e != nil {
		t.Fatal(e)
	}
	p.put.once.Do(func() { close(p.put.gate) })
	if e = <-errs; !errors.Is(e, ErrUpload) {
		t.Fatal("inflight upload resurrected", e)
	}
	if e = s.Collect(ctx); e != nil {
		t.Fatal(e)
	}
}

type cancelPrefetch struct {
	provider.Provider
	first   string
	started atomic.Int64
	stopped atomic.Int64
}

func (p *cancelPrefetch) Get(ctx context.Context, ref provider.Ref) ([]byte, error) {
	if ref.ID == p.first {
		return p.Provider.Get(ctx, ref)
	}
	p.started.Add(1)
	<-ctx.Done()
	p.stopped.Add(1)
	return nil, ctx.Err()
}
func TestClosingReaderCancelsAndJoinsPrefetch(t *testing.T) {
	s, _ := openTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte("x"), 4*ChunkSize)
	if _, e := s.Put(ctx, "test-bucket", "cancel", "", bytes.NewReader(data), ""); e != nil {
		t.Fatal(e)
	}
	rec, e := s.object("test-bucket", "cancel")
	if e != nil {
		t.Fatal(e)
	}
	p := &cancelPrefetch{Provider: s.Providers["local"], first: rec.Chunks[0].ID}
	s.Providers["local"] = p
	r, _, e := s.OpenReader(ctx, "test-bucket", "cancel")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Read(make([]byte, 1)); e != nil {
		t.Fatal(e)
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	if p.started.Load() != p.stopped.Load() || len(s.downloadSlots) != 0 || len(s.pins) != 0 {
		t.Fatal("close leaked prefetch operations or snapshot pin")
	}
}

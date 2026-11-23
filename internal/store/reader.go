package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sort"
	"sync"
)

// Reader fetches and authenticates only the chunk intersecting the requested offset.
// The pin protects its immutable snapshot from overwrite/delete garbage collection.
type Reader struct {
	store       *Store
	ctx         context.Context
	rec         record
	offsets     []int64
	pos         int64
	cache       map[int]*chunkFuture
	cancel      context.CancelFunc
	workers     sync.WaitGroup
	prefetch    int
	prefetchEnd int64
	tree        hash.Hash
	closed      bool
	Err         error
	digest      hash.Hash
	hashed      int64
	sequential  bool
}

func (s *Store) OpenReader(ctx context.Context, bucket, key string) (*Reader, Object, error) {
	s.mu.Lock()
	rec, e := s.object(bucket, key)
	if e != nil {
		s.mu.Unlock()
		return nil, Object{}, e
	}
	if s.Providers[rec.Object.Provider] == nil {
		s.mu.Unlock()
		return nil, Object{}, errors.New("provider is not configured")
	}
	s.pins[rec.Object.ID]++
	s.mu.Unlock()
	readCtx, cancel := context.WithCancel(ctx)
	r := &Reader{store: s, ctx: readCtx, rec: rec, cache: map[int]*chunkFuture{}, cancel: cancel, prefetch: s.DownloadConcurrency, prefetchEnd: rec.Object.Size - 1, digest: sha256.New(), tree: sha256.New(), sequential: true}
	var offset int64
	for _, chunk := range rec.Chunks {
		r.offsets = append(r.offsets, offset)
		size := chunk.Size
		if size == 0 {
			size = min(int64(ChunkSize), rec.Object.Size-offset)
		}
		if size < 0 || size > ChunkSize {
			r.Close()
			return nil, Object{}, errors.New("invalid chunk size")
		}
		offset += size
	}
	if offset != rec.Object.Size {
		r.Close()
		return nil, Object{}, errors.New("invalid object manifest")
	}
	return r, rec.Object, nil
}

type chunkFuture struct {
	done   chan struct{}
	cancel context.CancelFunc
	data   []byte
	err    error
}

// PrefetchThrough is a hint used by HTTP Range; correctness never depends on it.
func (r *Reader) PrefetchThrough(end int64) { r.prefetchEnd = end }
func (r *Reader) SingleChunkPrefetch()      { r.prefetch = 1 }
func (r *Reader) fetch(ctx context.Context, index int) ([]byte, error) {
	if e := acquire(ctx, r.store.downloadSlots); e != nil {
		return nil, e
	}
	defer func() { <-r.store.downloadSlots }()
	ref := r.rec.Chunks[index]
	encrypted, e := r.store.Providers[r.rec.Object.Provider].Get(ctx, ref.Ref)
	if e != nil {
		return nil, e
	}
	nonceSize := r.store.AEAD.NonceSize()
	if len(encrypted) < nonceSize {
		return nil, errors.New("corrupt chunk")
	}
	id, i := r.rec.Object.ID, index
	if ref.CipherID != "" {
		id, i = ref.CipherID, ref.Index
	}
	plain, e := r.store.AEAD.Open(encrypted[nonceSize:nonceSize], encrypted[:nonceSize], encrypted[nonceSize:], aad(id, i))
	if e != nil {
		return nil, errors.New("chunk authentication failed")
	}
	end := r.rec.Object.Size
	if index+1 < len(r.offsets) {
		end = r.offsets[index+1]
	}
	if int64(len(plain)) != end-r.offsets[index] {
		return nil, errors.New("chunk size mismatch")
	}
	if ref.SHA256 != "" {
		sum := sha256.Sum256(plain)
		if hex.EncodeToString(sum[:]) != ref.SHA256 {
			return nil, errors.New("chunk SHA-256 mismatch")
		}
	}
	return plain, nil
}
func (r *Reader) chunk(index int) ([]byte, error) {
	end := min(index+r.prefetch, len(r.offsets))
	for end > index+1 && r.offsets[end-1] > r.prefetchEnd {
		end--
	}
	for i, f := range r.cache {
		if i < index || i >= end {
			f.cancel()
			delete(r.cache, i)
		}
	}
	// Start the requested chunk first, then overlap following downloads.
	for i := index; i < end; i++ {
		if r.cache[i] != nil {
			continue
		}
		ctx, cancel := context.WithCancel(r.ctx)
		future := &chunkFuture{done: make(chan struct{}), cancel: cancel}
		r.cache[i] = future
		r.workers.Add(1)
		go func(n int, f *chunkFuture) { defer r.workers.Done(); f.data, f.err = r.fetch(ctx, n); close(f.done) }(i, future)
	}
	f := r.cache[index]
	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-f.done:
		return f.data, f.err
	}
}
func (r *Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("reader closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if e := r.ctx.Err(); e != nil {
		r.Err = e
		return 0, e
	}
	if r.pos >= r.rec.Object.Size {
		return 0, io.EOF
	}
	index := sort.Search(len(r.offsets), func(i int) bool { return r.offsets[i] > r.pos }) - 1
	chunk, e := r.chunk(index)
	if e != nil {
		r.Err = e
		return 0, e
	}
	start := r.pos - r.offsets[index]
	n := min(len(p), len(chunk)-int(start))
	next := r.pos + int64(n)
	if r.sequential && r.hashed == r.pos {
		_, _ = r.digest.Write(chunk[start : start+int64(n)])
		r.hashed = next
		end := r.rec.Object.Size
		if index+1 < len(r.offsets) {
			end = r.offsets[index+1]
		}
		if next == end && r.rec.Object.TreeSHA256 != "" {
			sum := sha256.Sum256(chunk)
			_, _ = r.tree.Write(sum[:])
			if next == r.rec.Object.Size && hex.EncodeToString(r.tree.Sum(nil)) != r.rec.Object.TreeSHA256 {
				r.Err = errors.New("object SHA-256 tree mismatch")
				return 0, r.Err
			}
		}
		if next == r.rec.Object.Size && r.rec.Object.SHA256 != "" && hex.EncodeToString(r.digest.Sum(nil)) != r.rec.Object.SHA256 {
			r.Err = errors.New("object SHA-256 mismatch")
			return 0, r.Err
		}
	} else {
		r.sequential = false
	}
	copy(p, chunk[start:start+int64(n)])
	r.pos = next
	return n, nil
}
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, errors.New("reader closed")
	}
	pos := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		pos = r.pos + offset
	case io.SeekEnd:
		pos = r.rec.Object.Size + offset
	default:
		return 0, errors.New("invalid seek")
	}
	if pos < 0 {
		return 0, errors.New("negative seek")
	}
	r.pos = pos
	// ServeContent probes EOF, then seeks back before reading. Preserve hashing until bytes are read.
	if r.hashed > 0 && pos != r.hashed {
		r.sequential = false
	}
	return pos, nil
}
func (r *Reader) Close() error {
	if !r.closed {
		r.cancel()
		for _, f := range r.cache {
			f.cancel()
		}
		r.workers.Wait()
		r.store.mu.Lock()
		r.store.pins[r.rec.Object.ID]--
		if r.store.pins[r.rec.Object.ID] == 0 {
			delete(r.store.pins, r.rec.Object.ID)
		}
		r.store.mu.Unlock()
		r.closed = true
		r.cache = nil
	}
	return nil
}

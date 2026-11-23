package store

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"

	"maxstore/internal/provider"
)

// uploadStream keeps at most N active blocks and one input block. Hashing on
// src remains ordered, while encryption and provider writes overlap.
func (s *Store) uploadStream(ctx context.Context, p provider.Provider, id string, src io.Reader) ([]Chunk, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type job struct {
		index, n int
		raw      []byte
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	var mu sync.Mutex
	stored := map[int]Chunk{}
	var failure error
	fail := func(e error) {
		mu.Lock()
		if failure == nil {
			failure = e
			cancel()
		}
		mu.Unlock()
	}
	for worker := 0; worker < s.UploadConcurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if e := acquire(ctx, s.uploadSlots); e != nil {
					fail(e)
					continue
				}
				nonceSize := s.AEAD.NonceSize()
				nonce := j.raw[:nonceSize]
				_, e := rand.Read(nonce)
				var ref provider.Ref
				if e == nil {
					sealed := s.AEAD.Seal(j.raw[:nonceSize], nonce, j.raw[nonceSize:nonceSize+j.n], aad(id, j.index))
					ref, e = p.Put(ctx, sealed)
				}
				<-s.uploadSlots
				if e != nil {
					fail(e)
					continue
				}
				mu.Lock()
				stored[j.index] = Chunk{Ref: ref, Size: int64(j.n)}
				mu.Unlock()
			}
		}()
	}
	var total int64
	index := 0
loop:
	for {
		if e := ctx.Err(); e != nil {
			fail(e)
			break
		}
		raw := make([]byte, s.AEAD.NonceSize()+ChunkSize+s.AEAD.Overhead())
		n, e := io.ReadFull(src, raw[s.AEAD.NonceSize():s.AEAD.NonceSize()+ChunkSize])
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			fail(e)
			break
		}
		if n == 0 {
			break
		}
		total += int64(n)
		if s.MaxSize > 0 && total > s.MaxSize {
			fail(ErrTooLarge)
			break
		}
		select {
		case jobs <- job{index, n, raw}:
			index++
		case <-ctx.Done():
			break loop
		}
		if n < ChunkSize {
			break
		}
	}
	close(jobs)
	wg.Wait()
	refs := make([]Chunk, 0, len(stored))
	for i := 0; i < index; i++ {
		if ref, ok := stored[i]; ok {
			refs = append(refs, ref)
		}
	}
	if failure == nil && len(refs) != index {
		failure = errors.New("upload pipeline lost a chunk")
	}
	return refs, total, failure
}

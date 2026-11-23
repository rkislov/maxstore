package store

import (
	"context"
	"errors"
)

type transferLock struct {
	gate chan struct{}
	refs int
}

func (s *Store) ActiveTransfers() (uploads, downloads int) {
	return len(s.uploadSlots), len(s.downloadSlots)
}

// ConfigureConcurrency is called once, before serving requests.
func (s *Store) ConfigureConcurrency(upload, download int) error {
	if upload < 1 || upload > 16 || download < 1 || download > 16 {
		return errors.New("transfer concurrency must be between 1 and 16")
	}
	s.UploadConcurrency, s.DownloadConcurrency = upload, download
	s.uploadSlots = make(chan struct{}, upload)
	s.downloadSlots = make(chan struct{}, download)
	return nil
}
func acquire(ctx context.Context, slots chan struct{}) error {
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Store) lockPart(ctx context.Context, key string) (func(), error) {
	s.mu.Lock()
	lock := s.partLocks[key]
	if lock == nil {
		lock = &transferLock{gate: make(chan struct{}, 1)}
		s.partLocks[key] = lock
	}
	lock.refs++
	s.mu.Unlock()
	releaseRef := func() {
		s.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.partLocks, key)
		}
		s.mu.Unlock()
	}
	if e := acquire(ctx, lock.gate); e != nil {
		releaseRef()
		return nil, e
	}
	return func() { <-lock.gate; releaseRef() }, nil
}

package server

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	hashpkg "hash"
	"hash/crc32"
	"io"
	"maxstore/internal/store"
	"net/http"
	"strings"
)

// payloadReader validates checksums before the store publishes a new object.
// It never buffers a complete object in memory.
type payloadReader struct {
	io.Reader
	hashes    map[string]hashpkg.Hash
	expected  map[string]string
	Checksums map[string]string
}

func newPayload(r *http.Request) *payloadReader {
	p := &payloadReader{hashes: map[string]hashpkg.Hash{}, expected: map[string]string{}, Checksums: map[string]string{}}
	for name, makeHash := range map[string]func() hashpkg.Hash{"Content-MD5": md5.New, "X-Amz-Checksum-Sha1": sha1.New, "X-Amz-Checksum-Sha256": sha256.New, "X-Amz-Checksum-Crc32": func() hashpkg.Hash { return crc32.NewIEEE() }, "X-Amz-Checksum-Crc32c": func() hashpkg.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) }} {
		if v := r.Header.Get(name); v != "" {
			p.hashes[name] = makeHash()
			p.expected[name] = v
		}
	}
	writers := []io.Writer{}
	for _, h := range p.hashes {
		writers = append(writers, h)
	}
	p.Reader = io.TeeReader(r.Body, io.MultiWriter(writers...))
	return p
}
func (p *payloadReader) Verify() error {
	for name, h := range p.hashes {
		sum := base64.StdEncoding.EncodeToString(h.Sum(nil))
		if sum != p.expected[name] {
			return store.ErrDigest
		}
		if name != "Content-MD5" {
			p.Checksums[name] = sum
		}
	}
	return nil
}
func readS3Small(r *http.Request, limit int64) ([]byte, error) {
	p := newPayload(r)
	b, e := io.ReadAll(io.LimitReader(p, limit+1))
	if e != nil {
		return nil, e
	}
	if int64(len(b)) > limit {
		return nil, store.ErrTooLarge
	}
	if e = p.Verify(); e != nil {
		return nil, e
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload != "" && payload != "UNSIGNED-PAYLOAD" && hashBody(b) != payload {
		return nil, store.ErrDigest
	}
	return b, nil
}
func hashBody(b []byte) string { return hash(string(b)) }

// CRC64NVME is intentionally rejected rather than silently ignored.
func unsupportedChecksum(r *http.Request) bool {
	return r.Header.Get("X-Amz-Checksum-Crc64nvme") != "" || strings.EqualFold(r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"), "CRC64NVME")
}

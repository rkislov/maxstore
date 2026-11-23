package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"maxstore/internal/provider"
)

const MaxS3PartSize int64 = 5 << 30

var ErrUpload = errors.New("upload not found or does not match object")
var ErrPart = errors.New("invalid part, checksum, size or order")

type Upload struct {
	ID        string    `json:"id"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	Type      string    `json:"type"`
	Provider  string    `json:"provider"`
	Mode      string    `json:"mode"`
	Size      int64     `json:"size"`
	Offset    int64     `json:"offset"`
	NextPart  int       `json:"next_part"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
	Completed *Object   `json:"completed,omitempty"`
}
type uploadRecord struct {
	Options ObjectOptions `json:"options,omitempty"`
	Upload
	MDState  []byte `json:"md_state,omitempty"`
	SHAState []byte `json:"sha_state,omitempty"`
}
type Part struct {
	Number   int       `json:"number"`
	Size     int64     `json:"size"`
	ETag     string    `json:"etag"`
	SHA256   string    `json:"sha256"`
	Modified time.Time `json:"modified"`
}
type partRecord struct {
	Part
	Chunks []Chunk `json:"chunks"`
}
type CompletedPart struct {
	Number int    `json:"number" xml:"PartNumber"`
	ETag   string `json:"etag" xml:"ETag"`
}

func partKey(id string, n int) string { return fmt.Sprintf("%s\x00%020d", id, n) }
func readUpload(tx *bolt.Tx, id string) (uploadRecord, error) {
	var u uploadRecord
	raw := tx.Bucket([]byte("uploads")).Get([]byte(id))
	if raw == nil {
		return u, ErrUpload
	}
	e := json.Unmarshal(raw, &u)
	return u, e
}
func (s *Store) Upload(id string) (Upload, error) {
	var u uploadRecord
	e := s.DB.View(func(tx *bolt.Tx) error { var e error; u, e = readUpload(tx, id); return e })
	return u.Upload, e
}
func (s *Store) CreateUpload(bucket, key, ctype, mode string, size int64, options ...ObjectOptions) (Upload, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !validKey(key) || (mode != "resumable" && mode != "parallel" && mode != "s3") || size < -1 || mode != "s3" && size < 0 {
		return Upload{}, ErrInvalid
	}
	if s.MaxSize > 0 && size > s.MaxSize {
		return Upload{}, ErrTooLarge
	}
	b, e := s.Bucket(bucket)
	if e != nil {
		return Upload{}, e
	}
	if s.Providers[b.Provider] == nil {
		return Upload{}, errors.New("provider is not configured")
	}
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	now := time.Now().UTC()
	u := uploadRecord{Upload: Upload{ID: provider.ID(), Bucket: bucket, Key: key, Type: ctype, Provider: b.Provider, Mode: mode, Size: size, NextPart: 1, Created: now, Updated: now}}
	if len(options) > 0 {
		u.Options = options[0]
	}
	u.MDState, _ = md5.New().(encoding.BinaryMarshaler).MarshalBinary()
	u.SHAState, _ = sha256.New().(encoding.BinaryMarshaler).MarshalBinary()
	e = s.DB.Update(func(tx *bolt.Tx) error { return putJSON(tx.Bucket([]byte("uploads")), u.ID, u) })
	return u.Upload, e
}
func (s *Store) Parts(id string) ([]Part, error) {
	out := []Part{}
	e := s.DB.View(func(tx *bolt.Tx) error {
		if _, e := readUpload(tx, id); e != nil {
			return e
		}
		return eachPart(tx, id, func(k []byte, p partRecord) error { out = append(out, p.Part); return nil })
	})
	return out, e
}
func eachPart(tx *bolt.Tx, id string, f func([]byte, partRecord) error) error {
	prefix := id + "\x00"
	c := tx.Bucket([]byte("parts")).Cursor()
	for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
		var p partRecord
		if e := json.Unmarshal(v, &p); e != nil {
			return e
		}
		if e := f(k, p); e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) UploadPart(ctx context.Context, id string, number int, src io.Reader, expectedSHA string, validators ...func() error) (Part, error) {
	unlock, e := s.lockPart(ctx, partKey(id, number))
	if e != nil {
		return Part{}, e
	}
	defer unlock()
	if e = acquire(ctx, s.uploadSlots); e != nil {
		return Part{}, e
	}
	defer func() { <-s.uploadSlots }()
	var u uploadRecord
	var old partRecord
	e = s.DB.View(func(tx *bolt.Tx) error {
		var e error
		u, e = readUpload(tx, id)
		if e != nil {
			return e
		}
		if raw := tx.Bucket([]byte("parts")).Get([]byte(partKey(id, number))); raw != nil {
			return json.Unmarshal(raw, &old)
		}
		return nil
	})
	if e != nil {
		return Part{}, e
	}
	if u.Completed != nil || number < 1 || u.Mode == "s3" && number > 10000 {
		return Part{}, ErrPart
	}
	p := s.Providers[u.Provider]
	if p == nil {
		return Part{}, errors.New("provider is not configured")
	}
	limit := MaxS3PartSize
	if u.Mode != "s3" {
		limit = ChunkSize
		if u.Mode == "resumable" && number > u.NextPart {
			return Part{}, ErrPart
		}
	}
	if u.Mode == "parallel" && (u.Size == 0 || int64(number-1) > (u.Size-1)/ChunkSize) {
		return Part{}, ErrPart
	}
	if (u.Mode == "resumable" && number < u.NextPart) || (u.Mode == "parallel" && old.Number != 0) {
		digest := sha256.New()
		n, e := io.Copy(digest, io.LimitReader(src, limit+1))
		if e != nil {
			return Part{}, e
		}
		sum := hex.EncodeToString(digest.Sum(nil))
		if n != old.Size || sum != old.SHA256 || !validSHA(expectedSHA, sum) {
			return Part{}, ErrPart
		}
		for _, validate := range validators {
			if e := validate(); e != nil {
				return Part{}, e
			}
		}
		return old.Part, nil
	}
	md, sha := md5.New(), sha256.New()
	writers := []io.Writer{md, sha}
	var wholeMD, wholeSHA hash.Hash
	if u.Mode == "resumable" {
		wholeMD = md5.New()
		wholeSHA = sha256.New()
		if e = wholeMD.(encoding.BinaryUnmarshaler).UnmarshalBinary(u.MDState); e != nil {
			return Part{}, e
		}
		if e = wholeSHA.(encoding.BinaryUnmarshaler).UnmarshalBinary(u.SHAState); e != nil {
			return Part{}, e
		}
		writers = append(writers, wholeMD, wholeSHA)
	}
	reader := io.TeeReader(io.LimitReader(src, limit+1), io.MultiWriter(writers...))
	buf := make([]byte, ChunkSize)
	refs := []Chunk{}
	cipherID := provider.ID()
	var size int64
	committed := false
	defer func() {
		if !committed {
			_ = s.enqueue(u.Provider, refs)
		}
	}()
	for {
		n, readErr := io.ReadFull(reader, buf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return Part{}, readErr
		}
		if n == 0 {
			break
		}
		size += int64(n)
		if size > limit {
			return Part{}, ErrTooLarge
		}
		total := u.Offset - old.Size + size
		if s.MaxSize > 0 && total > s.MaxSize || u.Mode != "s3" && total > u.Size {
			return Part{}, ErrTooLarge
		}
		nonce := make([]byte, s.AEAD.NonceSize())
		if _, e = rand.Read(nonce); e != nil {
			return Part{}, e
		}
		sealed := s.AEAD.Seal(nonce, nonce, buf[:n], aad(cipherID, len(refs)))
		ref, e := p.Put(ctx, sealed)
		if e != nil {
			return Part{}, e
		}
		chunkSHA := sha256.Sum256(buf[:n])
		refs = append(refs, Chunk{Ref: ref, CipherID: cipherID, Index: len(refs), Size: int64(n), SHA256: hex.EncodeToString(chunkSHA[:])})
		if n < ChunkSize {
			break
		}
	}
	sum := hex.EncodeToString(sha.Sum(nil))
	if !validSHA(expectedSHA, sum) {
		return Part{}, ErrPart
	}
	if u.Mode == "resumable" && (size == 0 || u.Offset+size < u.Size && size != ChunkSize) {
		return Part{}, ErrPart
	}
	if u.Mode == "parallel" {
		expected := min(int64(ChunkSize), u.Size-int64(number-1)*ChunkSize)
		if size != expected {
			return Part{}, ErrPart
		}
	}
	for _, validate := range validators {
		if e := validate(); e != nil {
			return Part{}, e
		}
	}
	part := Part{Number: number, Size: size, ETag: hex.EncodeToString(md.Sum(nil)), SHA256: sum, Modified: time.Now().UTC()}
	s.writeMu.Lock()
	e = s.DB.Update(func(tx *bolt.Tx) error {
		current, err := readUpload(tx, id)
		if err != nil {
			return err
		}
		if current.Completed != nil {
			return ErrUpload
		}
		if u.Mode == "resumable" && current.NextPart != number {
			return ErrPart
		}
		total := current.Offset + size - old.Size
		if s.MaxSize > 0 && total > s.MaxSize {
			return ErrTooLarge
		}
		current.Offset = total
		current.Updated = time.Now().UTC()
		if u.Mode == "resumable" {
			current.NextPart++
			current.MDState, _ = wholeMD.(encoding.BinaryMarshaler).MarshalBinary()
			current.SHAState, _ = wholeSHA.(encoding.BinaryMarshaler).MarshalBinary()
		}
		if err = queue(tx, u.Provider, old.Chunks); err != nil {
			return err
		}
		parts := tx.Bucket([]byte("parts"))
		if err = putJSON(parts, partKey(id, number), partRecord{part, refs}); err != nil {
			return err
		}
		if u.Mode == "parallel" {
			for parts.Get([]byte(partKey(id, current.NextPart))) != nil {
				current.NextPart++
			}
		}
		return putJSON(tx.Bucket([]byte("uploads")), id, current)
	})
	s.writeMu.Unlock()
	if e != nil {
		return Part{}, e
	}
	committed = true
	return part, nil
}
func validSHA(expected, actual string) bool {
	return expected == "" || expected == "UNSIGNED-PAYLOAD" || expected == actual
}
func (s *Store) CompleteUpload(id string, selected []CompletedPart, conditions ...ObjectOptions) (Object, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var obj Object
	e := s.DB.Update(func(tx *bolt.Tx) error {
		u, e := readUpload(tx, id)
		if e != nil {
			return e
		}
		if u.Completed != nil {
			obj = *u.Completed
			return nil
		}
		if tx.Bucket([]byte("buckets")).Get([]byte(u.Bucket)) == nil {
			return ErrNotFound
		}
		all := map[int]partRecord{}
		if e = eachPart(tx, id, func(k []byte, p partRecord) error { all[p.Number] = p; return nil }); e != nil {
			return e
		}
		if u.Mode != "s3" {
			if u.Offset != u.Size {
				return ErrPart
			}
			selected = []CompletedPart{}
			expectedParts := u.Size / ChunkSize
			if u.Size%ChunkSize != 0 {
				expectedParts++
			}
			for i := 1; int64(i) <= expectedParts; i++ {
				p, ok := all[i]
				if !ok {
					return ErrPart
				}
				selected = append(selected, CompletedPart{i, p.ETag})
			}
		} else if len(selected) == 0 || len(selected) > 10000 {
			return ErrPart
		}
		obj = Object{Tags: u.Options.Tags, Metadata: u.Options.Metadata, Headers: u.Options.Headers, Bucket: u.Bucket, Key: u.Key, ID: u.ID, Type: u.Type, Provider: u.Provider, Created: time.Now().UTC()}
		chunks := []Chunk{}
		composite := md5.New()
		tree := sha256.New()
		last := 0
		chosen := map[int]bool{}
		for i, p := range selected {
			part, ok := all[p.Number]
			if !ok || p.Number <= last || strings.Trim(p.ETag, `"`) != part.ETag {
				return ErrPart
			}
			if u.Mode == "s3" && i < len(selected)-1 && part.Size < 5<<20 {
				return ErrPart
			}
			last = p.Number
			chosen[p.Number] = true
			obj.Size += part.Size
			chunks = append(chunks, part.Chunks...)
			raw, e := hex.DecodeString(part.ETag)
			if e != nil {
				return e
			}
			_, _ = composite.Write(raw)
			if u.Mode == "parallel" {
				checksum, e := hex.DecodeString(part.SHA256)
				if e != nil || len(checksum) != 32 {
					return ErrPart
				}
				_, _ = tree.Write(checksum)
			}
		}
		if s.MaxSize > 0 && obj.Size > s.MaxSize {
			return ErrTooLarge
		}
		obj.ETag = fmt.Sprintf("%x-%d", composite.Sum(nil), len(selected))
		if u.Mode == "parallel" {
			obj.TreeSHA256 = hex.EncodeToString(tree.Sum(nil))
		}
		if u.Mode == "resumable" {
			md, sha := md5.New(), sha256.New()
			if e = md.(encoding.BinaryUnmarshaler).UnmarshalBinary(u.MDState); e != nil {
				return e
			}
			if e = sha.(encoding.BinaryUnmarshaler).UnmarshalBinary(u.SHAState); e != nil {
				return e
			}
			obj.ETag = hex.EncodeToString(md.Sum(nil))
			obj.SHA256 = hex.EncodeToString(sha.Sum(nil))
		}
		objects := tx.Bucket([]byte("objects"))
		k := u.Bucket + "\x00" + u.Key
		opts := u.Options
		if len(conditions) > 0 {
			opts.IfMatch = conditions[0].IfMatch
			opts.IfNoneMatch = conditions[0].IfNoneMatch
		}
		if e := checkWrite(objects, k, opts); e != nil {
			return e
		}
		if raw := objects.Get([]byte(k)); raw != nil {
			var old record
			if e = json.Unmarshal(raw, &old); e != nil {
				return e
			}
			if e = queue(tx, old.Object.Provider, old.Chunks, old.Object.ID); e != nil {
				return e
			}
		}
		for n, p := range all {
			if !chosen[n] {
				if e = queue(tx, u.Provider, p.Chunks); e != nil {
					return e
				}
			}
			if e = tx.Bucket([]byte("parts")).Delete([]byte(partKey(id, n))); e != nil {
				return e
			}
		}
		if e = putJSON(objects, k, record{obj, chunks}); e != nil {
			return e
		}
		u.Completed = &obj
		u.MDState = nil
		u.SHAState = nil
		u.Updated = time.Now().UTC()
		return putJSON(tx.Bucket([]byte("uploads")), id, u)
	})
	return obj, e
}
func abortUpload(tx *bolt.Tx, id string) error {
	u, e := readUpload(tx, id)
	if e != nil {
		return e
	}
	if u.Completed != nil {
		return tx.Bucket([]byte("uploads")).Delete([]byte(id))
	}
	keys := []string{}
	if e = eachPart(tx, id, func(k []byte, p partRecord) error {
		keys = append(keys, string(k))
		return queue(tx, u.Provider, p.Chunks)
	}); e != nil {
		return e
	}
	for _, k := range keys {
		if e = tx.Bucket([]byte("parts")).Delete([]byte(k)); e != nil {
			return e
		}
	}
	return tx.Bucket([]byte("uploads")).Delete([]byte(id))
}
func (s *Store) AbortUpload(id string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error { return abortUpload(tx, id) })
}
func (s *Store) ExpireUploads(now time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		ids := []string{}
		if e := tx.Bucket([]byte("uploads")).ForEach(func(k, v []byte) error {
			var u uploadRecord
			if e := json.Unmarshal(v, &u); e != nil {
				return e
			}
			ttl := 7 * 24 * time.Hour
			if u.Completed != nil {
				ttl = 24 * time.Hour
			}
			if now.Sub(u.Updated) > ttl {
				ids = append(ids, string(k))
			}
			return nil
		}); e != nil {
			return e
		}
		for _, id := range ids {
			if e := abortUpload(tx, id); e != nil {
				return e
			}
		}
		return nil
	})
}
func (s *Store) Uploads(bucket string) ([]Upload, error) {
	out := []Upload{}
	e := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("uploads")).ForEach(func(k, v []byte) error {
			var u uploadRecord
			if e := json.Unmarshal(v, &u); e != nil {
				return e
			}
			if u.Bucket == bucket && u.Completed == nil {
				out = append(out, u.Upload)
			}
			return nil
		})
	})
	return out, e
}

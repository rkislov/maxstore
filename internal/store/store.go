package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
	"maxstore/internal/provider"
)

const ChunkSize = 8 << 20

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("bucket already exists or is not empty")
var ErrInvalid = errors.New("invalid name or provider")
var ErrTooLarge = errors.New("object exceeds upload limit")

type Bucket struct {
	Tags     map[string]string `json:"tags,omitempty"`
	Name     string            `json:"name"`
	Provider string            `json:"provider"`
	Created  time.Time         `json:"created"`
}
type Object struct {
	Tags       map[string]string `json:"tags,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Checksums  map[string]string `json:"checksums,omitempty"`
	TreeSHA256 string            `json:"tree_sha256,omitempty"`
	Bucket     string            `json:"bucket"`
	Key        string            `json:"key"`
	ID         string            `json:"id"`
	Size       int64             `json:"size"`
	Type       string            `json:"type"`
	ETag       string            `json:"etag"`
	SHA256     string            `json:"sha256"`
	Created    time.Time         `json:"created"`
	Provider   string            `json:"provider"`
	Chunks     []provider.Ref    `json:"-"`
}

// Separate disk representation keeps remote references out of the public API.
type Chunk struct {
	provider.Ref
	CipherID string `json:"cipher_id,omitempty"`
	Index    int    `json:"index,omitempty"`
	Size     int64  `json:"size,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
}

type record struct {
	Object Object  `json:"object"`
	Chunks []Chunk `json:"chunks"`
}
type garbage struct {
	Provider string
	Ref      provider.Ref
	OwnerID  string
}
type Store struct {
	DB                  *bolt.DB
	Providers           map[string]provider.Provider
	AEAD                cipher.AEAD
	Dir                 string
	MaxSize             int64
	mu                  sync.Mutex
	writeMu             sync.Mutex
	gcMu                sync.Mutex
	pins                map[string]int
	UploadConcurrency   int
	DownloadConcurrency int
	uploadSlots         chan struct{}
	downloadSlots       chan struct{}
	partLocks           map[string]*transferLock
}

func Open(dir string, providers map[string]provider.Provider, maxSize int64) (*Store, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	keyPath := filepath.Join(dir, "master.key")
	key, e := os.ReadFile(keyPath)
	if os.IsNotExist(e) {
		key = make([]byte, 32)
		if _, e = rand.Read(key); e != nil {
			return nil, e
		}
		f, err := os.OpenFile(keyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		_, e = f.Write(key)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e == nil {
			e = ce
		}
	}
	if e != nil {
		return nil, e
	}
	if len(key) != 32 {
		return nil, errors.New("master.key must contain exactly 32 bytes")
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	aead, e := cipher.NewGCM(block)
	if e != nil {
		return nil, e
	}
	db, e := bolt.Open(filepath.Join(dir, "metadata.db"), 0600, &bolt.Options{Timeout: time.Second})
	if e != nil {
		return nil, e
	}
	e = db.Update(func(tx *bolt.Tx) error {
		for _, n := range []string{"buckets", "objects", "garbage", "uploads", "parts", "auth", "sessions"} {
			if _, e := tx.CreateBucketIfNotExists([]byte(n)); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Store{DB: db, Providers: providers, AEAD: aead, Dir: dir, MaxSize: maxSize, pins: make(map[string]int), UploadConcurrency: 4, DownloadConcurrency: 4, uploadSlots: make(chan struct{}, 4), downloadSlots: make(chan struct{}, 4), partLocks: make(map[string]*transferLock)}, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func putJSON(b *bolt.Bucket, k string, v any) error {
	raw, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return b.Put([]byte(k), raw)
}
func (s *Store) Buckets() ([]Bucket, error) {
	out := []Bucket{}
	e := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("buckets")).ForEach(func(k, v []byte) error {
			var b Bucket
			if e := json.Unmarshal(v, &b); e != nil {
				return e
			}
			out = append(out, b)
			return nil
		})
	})
	return out, e
}

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func (s *Store) CreateBucket(name, p string) error {
	if !namePattern.MatchString(name) || s.Providers[p] == nil {
		return ErrInvalid
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("buckets"))
		if b.Get([]byte(name)) != nil {
			return ErrConflict
		}
		return putJSON(b, name, Bucket{Name: name, Provider: p, Created: time.Now().UTC()})
	})
}
func (s *Store) Bucket(name string) (Bucket, error) {
	var b Bucket
	e := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("buckets")).Get([]byte(name))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &b)
	})
	return b, e
}
func (s *Store) DeleteBucket(name string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("buckets"))
		if b.Get([]byte(name)) == nil {
			return ErrNotFound
		}
		if e := tx.Bucket([]byte("uploads")).ForEach(func(k, v []byte) error {
			var u Upload
			if e := json.Unmarshal(v, &u); e != nil {
				return e
			}
			if u.Bucket == name && u.Completed == nil {
				return ErrConflict
			}
			return nil
		}); e != nil {
			return e
		}
		prefix := name + "\x00"
		k, _ := tx.Bucket([]byte("objects")).Cursor().Seek([]byte(prefix))
		if len(k) >= len(prefix) && string(k[:len(prefix)]) == prefix {
			return ErrConflict
		}
		return b.Delete([]byte(name))
	})
}
func (s *Store) Objects(bucket, prefix string) ([]Object, error) {
	if _, e := s.Bucket(bucket); e != nil {
		return nil, e
	}
	out := []Object{}
	p := []byte(bucket + "\x00" + prefix)
	e := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("objects")).Cursor()
		for k, v := c.Seek(p); k != nil && len(k) >= len(p) && string(k[:len(p)]) == string(p); k, v = c.Next() {
			var r record
			if e := json.Unmarshal(v, &r); e != nil {
				return e
			}
			out = append(out, r.Object)
		}
		return nil
	})
	return out, e
}
func (s *Store) object(bucket, key string) (record, error) {
	var r record
	e := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("objects")).Get([]byte(bucket + "\x00" + key))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &r)
	})
	return r, e
}
func (s *Store) Head(bucket, key string) (Object, error) {
	r, e := s.object(bucket, key)
	return r.Object, e
}
func validKey(key string) bool {
	return key != "" && len(key) <= 1024 && utf8.ValidString(key) && !regexp.MustCompile(`[\x00-\x1f\x7f]`).MatchString(key)
}
func aad(id string, index int) []byte { return []byte(fmt.Sprintf("maxstore/v1/%s/%d", id, index)) }
func (s *Store) Put(ctx context.Context, bucket, key, ctype string, src io.Reader, expectedSHA string, options ...ObjectOptions) (Object, error) {
	unlock, e := s.lockPart(ctx, "object:"+bucket+"\x00"+key)
	if e != nil {
		return Object{}, e
	}
	defer unlock()
	if !validKey(key) {
		return Object{}, ErrInvalid
	}
	b, e := s.Bucket(bucket)
	if e != nil {
		return Object{}, e
	}
	p := s.Providers[b.Provider]
	if p == nil {
		return Object{}, errors.New("provider is not configured")
	}
	opts := ObjectOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	obj := Object{Tags: opts.Tags, Metadata: opts.Metadata, Headers: opts.Headers, Checksums: opts.Checksums, Bucket: bucket, Key: key, ID: provider.ID(), Type: ctype, Provider: b.Provider, Created: time.Now().UTC()}
	if obj.Type == "" {
		obj.Type = "application/octet-stream"
	}
	refs := []Chunk{}
	committed := false
	defer func() {
		if !committed {
			_ = s.enqueue(b.Provider, refs)
		}
	}()
	md := md5.New()
	sha := sha256.New()
	var limited io.Reader = src
	if s.MaxSize > 0 {
		limited = io.LimitReader(src, s.MaxSize+1)
	}
	reader := io.TeeReader(limited, io.MultiWriter(md, sha))
	refs, obj.Size, e = s.uploadStream(ctx, p, obj.ID, reader)
	if e != nil {
		return Object{}, e
	}
	obj.ETag = hex.EncodeToString(md.Sum(nil))
	obj.SHA256 = hex.EncodeToString(sha.Sum(nil))
	if expectedSHA != "" && expectedSHA != "UNSIGNED-PAYLOAD" && expectedSHA != obj.SHA256 {
		return Object{}, errors.New("payload SHA256 mismatch")
	}
	if opts.Validate != nil {
		if e := opts.Validate(); e != nil {
			return Object{}, e
		}
	}
	s.writeMu.Lock()
	e = s.DB.Update(func(tx *bolt.Tx) error {
		var current Bucket
		raw := tx.Bucket([]byte("buckets")).Get([]byte(bucket))
		if raw == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if current.Provider != b.Provider || !current.Created.Equal(b.Created) {
			return ErrNotFound
		}
		objects := tx.Bucket([]byte("objects"))
		k := bucket + "\x00" + key
		if e := checkWrite(objects, k, opts); e != nil {
			return e
		}
		if old := objects.Get([]byte(k)); old != nil {
			var r record
			if e := json.Unmarshal(old, &r); e != nil {
				return e
			}
			if e := queue(tx, r.Object.Provider, r.Chunks, r.Object.ID); e != nil {
				return e
			}
		}
		return putJSON(objects, k, record{obj, refs})
	})
	s.writeMu.Unlock()
	if e != nil {
		return Object{}, e
	}
	committed = true
	return obj, nil
}
func queue(tx *bolt.Tx, p string, refs []Chunk, owner ...string) error {
	id := ""
	if len(owner) > 0 {
		id = owner[0]
	}
	for _, r := range refs {
		if e := putJSON(tx.Bucket([]byte("garbage")), provider.ID(), garbage{Provider: p, Ref: r.Ref, OwnerID: id}); e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) enqueue(p string, refs []Chunk) error {
	return s.DB.Update(func(tx *bolt.Tx) error { return queue(tx, p, refs) })
}
func (s *Store) Delete(bucket, key string) error {
	if !validKey(key) {
		return ErrInvalid
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket([]byte("objects"))
		k := bucket + "\x00" + key
		v := objects.Get([]byte(k))
		if v == nil {
			return nil
		}
		var r record
		if e := json.Unmarshal(v, &r); e != nil {
			return e
		}
		if e := queue(tx, r.Object.Provider, r.Chunks, r.Object.ID); e != nil {
			return e
		}
		return objects.Delete([]byte(k))
	})
}

func (s *Store) PendingGC() int {
	n := 0
	_ = s.DB.View(func(tx *bolt.Tx) error { n = tx.Bucket([]byte("garbage")).Stats().KeyN; return nil })
	return n
}
func (s *Store) Collect(ctx context.Context) error {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	type item struct {
		k string
		g garbage
	}
	items := []item{}
	e := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("garbage")).ForEach(func(k, v []byte) error {
			if len(items) >= 100 {
				return nil
			}
			var g garbage
			if e := json.Unmarshal(v, &g); e != nil {
				return e
			}
			items = append(items, item{string(k), g})
			return nil
		})
	})
	if e != nil {
		return e
	}
	var errs []error
	for _, i := range items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p := s.Providers[i.g.Provider]
		if p == nil {
			continue
		}
		s.mu.Lock()
		pinned := s.pins[i.g.OwnerID] > 0
		s.mu.Unlock()
		if pinned {
			continue
		}
		if e := p.Delete(ctx, i.g.Ref); e != nil {
			errs = append(errs, e)
			continue
		}
		if e := s.DB.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("garbage")).Delete([]byte(i.k)) }); e != nil {
			return e
		}
	}
	return errors.Join(errs...)
}
func (s *Store) ProviderNames() []string {
	names := []string{}
	for n := range s.Providers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

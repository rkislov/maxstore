package store

import (
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"strings"
)

var ErrPrecondition = errors.New("object precondition failed")
var ErrDigest = errors.New("request checksum mismatch")

type ObjectOptions struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Checksums   map[string]string `json:"checksums,omitempty"`
	IfMatch     string            `json:"if_match,omitempty"`
	IfNoneMatch string            `json:"if_none_match,omitempty"`
	Validate    func() error      `json:"-"`
}

func conditionMatches(value, etag string, exists, weak bool) bool {
	for _, candidate := range strings.Split(value, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return exists
		}
		if strings.HasPrefix(candidate, "W/") {
			if !weak {
				continue
			}
			candidate = strings.TrimPrefix(candidate, "W/")
		}
		if exists && strings.Trim(candidate, `"`) == etag {
			return true
		}
	}
	return false
}
func checkWrite(b *bolt.Bucket, key string, opts ObjectOptions) error {
	raw := b.Get([]byte(key))
	var old record
	if raw != nil {
		if e := json.Unmarshal(raw, &old); e != nil {
			return e
		}
	}
	if opts.IfMatch != "" && !conditionMatches(opts.IfMatch, old.Object.ETag, raw != nil, false) {
		return ErrPrecondition
	}
	if opts.IfNoneMatch != "" && conditionMatches(opts.IfNoneMatch, old.Object.ETag, raw != nil, true) {
		return ErrPrecondition
	}
	return nil
}

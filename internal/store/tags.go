package store

import (
	"encoding/json"
	bolt "go.etcd.io/bbolt"
)

// SetTags modifies metadata atomically without reuploading encrypted content.
func (s *Store) SetTags(bucket, key string, tags map[string]string) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		if key == "" {
			b := tx.Bucket([]byte("buckets"))
			raw := b.Get([]byte(bucket))
			if raw == nil {
				return ErrNotFound
			}
			var v Bucket
			if e := json.Unmarshal(raw, &v); e != nil {
				return e
			}
			v.Tags = tags
			return putJSON(b, bucket, v)
		}
		b := tx.Bucket([]byte("objects"))
		id := bucket + "\x00" + key
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		var v record
		if e := json.Unmarshal(raw, &v); e != nil {
			return e
		}
		v.Object.Tags = tags
		return putJSON(b, id, v)
	})
}

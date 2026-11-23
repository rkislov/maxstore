package provider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// Ref contains durable provider identifiers, never temporary download URLs.
type Ref struct {
	ID      string `json:"id"`
	Message string `json:"message,omitempty"`
}
type Provider interface {
	Put(context.Context, []byte) (Ref, error)
	Get(context.Context, Ref) ([]byte, error)
	Delete(context.Context, Ref) error
}

func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type Local struct{ Dir string }

func (l *Local) Put(ctx context.Context, b []byte) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	id := ID()
	if err := os.MkdirAll(l.Dir, 0700); err != nil {
		return Ref{}, err
	}
	path := filepath.Join(l.Dir, id)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Ref{}, err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return Ref{}, err
	}
	d, err := os.Open(l.Dir)
	if err != nil {
		_ = os.Remove(path)
		return Ref{}, err
	}
	err = d.Sync()
	_ = d.Close()
	if err != nil {
		_ = os.Remove(path)
		return Ref{}, err
	}
	return Ref{ID: id}, nil
}
func (l *Local) Get(ctx context.Context, r Ref) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validID(r.ID) {
		return nil, errors.New("invalid chunk ID")
	}
	return os.ReadFile(filepath.Join(l.Dir, r.ID))
}
func (l *Local) Delete(ctx context.Context, r Ref) error {
	if !validID(r.ID) {
		return errors.New("invalid chunk ID")
	}
	err := os.Remove(filepath.Join(l.Dir, r.ID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
func validID(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 16 }

package server

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/crypto/bcrypt"
	"maxstore/internal/provider"
)

const sessionCookie = "maxstore_session"
const sessionLifetime = 8 * time.Hour

type account struct {
	Username string
	Hash     []byte
}
type loginSession struct {
	Username string    `json:"username"`
	Expires  time.Time `json:"expires"`
}
type attempts struct {
	Count int
	Until time.Time
}
type ConsoleAuth struct {
	db       *bolt.DB
	account  account
	Secure   bool
	mu       sync.Mutex
	failures map[string]attempts
}

// Bootstrap credentials are provided only when there is no account yet. Passwords
// are salted with bcrypt; existing accounts survive application restarts.
func NewConsoleAuth(db *bolt.DB, username, password string, secure bool) (*ConsoleAuth, error) {
	var stored account
	e := db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("auth")).Get([]byte("admin"))
		if raw != nil {
			return json.Unmarshal(raw, &stored)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	if stored.Username == "" {
		username = strings.TrimSpace(username)
		if username == "" || len(username) > 128 || len(password) < 12 || len(password) > 72 {
			return nil, errors.New("admin username required; password must be 12–72 bytes")
		}
		hashed, e := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if e != nil {
			return nil, e
		}
		stored = account{username, hashed}
		raw, e := json.Marshal(stored)
		if e != nil {
			return nil, e
		}
		if e = db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("auth")).Put([]byte("admin"), raw) }); e != nil {
			return nil, e
		}
	}
	return &ConsoleAuth{db: db, account: stored, Secure: secure, failures: map[string]attempts{}}, nil
}
func (a *ConsoleAuth) session(r *http.Request) (loginSession, bool) {
	var sess loginSession
	cookie, e := r.Cookie(sessionCookie)
	if e != nil || len(cookie.Value) != 64 {
		return sess, false
	}
	e = a.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("sessions")).Get([]byte(hash(cookie.Value)))
		if raw == nil {
			return errors.New("missing session")
		}
		return json.Unmarshal(raw, &sess)
	})
	return sess, e == nil && time.Now().Before(sess.Expires)
}
func sameOrigin(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		u, e := url.Parse(origin)
		return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host == r.Host
	}
	return true
}
func (a *ConsoleAuth) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !sameOrigin(r) {
		jsonReply(w, 403, map[string]string{"error": "Недопустимый источник запроса"})
		return
	}
	if r.URL.Path == "/api/auth/session" && r.Method == "GET" {
		if sess, ok := a.session(r); ok {
			jsonReply(w, 200, sess)
		} else {
			jsonReply(w, 401, map[string]string{"error": "Войдите в консоль"})
		}
		return
	}
	if r.Method != "POST" {
		jsonReply(w, 405, map[string]string{"error": "Метод не поддерживается"})
		return
	}
	if r.URL.Path == "/api/auth/logout" {
		if r.Header.Get("X-Maxstore-Request") != "1" {
			jsonReply(w, 403, map[string]string{"error": "Недопустимый запрос"})
			return
		}
		if cookie, e := r.Cookie(sessionCookie); e == nil {
			_ = a.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("sessions")).Delete([]byte(hash(cookie.Value))) })
		}
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Path: "/", MaxAge: -1, HttpOnly: true, Secure: a.Secure || r.TLS != nil, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
		return
	}
	if r.URL.Path != "/api/auth/login" {
		jsonReply(w, 404, map[string]string{"error": "Маршрут не найден"})
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		jsonReply(w, 415, map[string]string{"error": "Ожидается JSON"})
		return
	}
	ip, _, e := net.SplitHostPort(r.RemoteAddr)
	if e != nil {
		ip = r.RemoteAddr
	}
	now := time.Now()
	a.mu.Lock()
	for k, v := range a.failures {
		if now.After(v.Until) {
			delete(a.failures, k)
		}
	}
	entry := a.failures[ip]
	blocked := entry.Count >= 8 || len(a.failures) >= 4096
	if !blocked {
		if entry.Count == 0 {
			entry.Until = now.Add(15 * time.Minute)
		}
		entry.Count++
		a.failures[ip] = entry
	}
	a.mu.Unlock()
	if blocked {
		w.Header().Set("Retry-After", "900")
		jsonReply(w, 429, map[string]string{"error": "Слишком много попыток входа. Повторите через 15 минут."})
		return
	}
	var v struct{ Username, Password string }
	if e := json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&v); e != nil || len(v.Password) > 72 {
		jsonReply(w, 400, map[string]string{"error": "Некорректные данные входа"})
		return
	}
	// Compare the hash even for an unknown username to avoid a username timing oracle.
	passErr := bcrypt.CompareHashAndPassword(a.account.Hash, []byte(v.Password))
	if strings.TrimSpace(v.Username) != a.account.Username || passErr != nil {
		jsonReply(w, 401, map[string]string{"error": "Неверный логин или пароль"})
		return
	}
	a.mu.Lock()
	delete(a.failures, ip)
	a.mu.Unlock()
	value := provider.ID() + provider.ID()
	sess := loginSession{a.account.Username, time.Now().Add(sessionLifetime)}
	e = a.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("sessions"))
		expired := [][]byte{}
		if e := b.ForEach(func(k, v []byte) error {
			var old loginSession
			if json.Unmarshal(v, &old) != nil || now.After(old.Expires) {
				expired = append(expired, append([]byte(nil), k...))
			}
			return nil
		}); e != nil {
			return e
		}
		for _, k := range expired {
			if e := b.Delete(k); e != nil {
				return e
			}
		}
		raw, e := json.Marshal(sess)
		if e != nil {
			return e
		}
		return b.Put([]byte(hash(value)), raw)
	})
	if e != nil {
		jsonReply(w, 500, map[string]string{"error": "Не удалось создать сессию"})
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: value, Path: "/", MaxAge: int(sessionLifetime.Seconds()), Expires: sess.Expires, HttpOnly: true, Secure: a.Secure || r.TLS != nil, SameSite: http.SameSiteStrictMode})
	jsonReply(w, 200, sess)
}

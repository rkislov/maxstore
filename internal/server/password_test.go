package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func authApp(t *testing.T) *Server {
	a := testApp(t)
	var e error
	a.Auth, e = NewConsoleAuth(a.Store.DB, "admin", "a-strong-test-password", false)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func loginRequest(h http.Handler, body string, remote string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "http://localhost/api/auth/login", strings.NewReader(body))
	r.RemoteAddr = remote
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestPasswordSessionLifecycleAndCSRF(t *testing.T) {
	a := authApp(t)
	h := a.UI()
	w := loginRequest(h, `{"username":"admin","password":"wrong"}`, "127.0.0.1:1234")
	if w.Code != 401 {
		t.Fatal("bad password accepted", w.Code)
	}
	w = loginRequest(h, `{"username":"admin","password":"a-strong-test-password"}`, "127.0.0.1:1234")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode || cookies[0].MaxAge != 28800 {
		t.Fatal("unsafe session cookie", cookies)
	}
	cookie := cookies[0]
	request := func(method, path, body, csrf string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://localhost"+path, strings.NewReader(body))
		r.AddCookie(cookie)
		if csrf != "" {
			r.Header.Set("X-Maxstore-Request", csrf)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if got := request("GET", "/api/status", "", ""); got.Code != 200 {
		t.Fatal("session rejected")
	}
	if got := request("POST", "/api/buckets", `{"name":"my-files","provider":"local"}`, ""); got.Code != 403 {
		t.Fatal("CSRF allowed")
	}
	if got := request("POST", "/api/buckets", `{"name":"my-files","provider":"local"}`, "1"); got.Code != 201 {
		t.Fatal(got.Code, got.Body.String())
	}
	next, e := NewConsoleAuth(a.Store.DB, "ignored", "different-password", false)
	if e != nil {
		t.Fatal(e)
	}
	a.Auth = next
	h = a.UI()
	if got := request("GET", "/api/auth/session", "", ""); got.Code != 200 {
		t.Fatal("session did not survive auth reload")
	}
	if got := request("POST", "/api/auth/logout", "", "1"); got.Code != 204 {
		t.Fatal("logout failed")
	}
	if got := request("GET", "/api/status", "", ""); got.Code != 401 {
		t.Fatal("logout did not revoke session")
	}
}
func TestLoginRateLimitAndOrigin(t *testing.T) {
	a := authApp(t)
	h := a.UI()
	for i := 0; i < 8; i++ {
		w := loginRequest(h, `{"username":"admin","password":"wrong"}`, "127.0.0.1:1234")
		if w.Code != 401 {
			t.Fatal("early rate limit", w.Code)
		}
	}
	if w := loginRequest(h, `{"username":"admin","password":"a-strong-test-password"}`, "127.0.0.1:1234"); w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal("missing rate limit")
	}
	r := httptest.NewRequest("POST", "http://localhost/api/auth/login", strings.NewReader(`{"username":"admin","password":"a-strong-test-password"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cross-origin login accepted")
	}
}
func TestResumableConsoleAPIWithCookie(t *testing.T) {
	a := authApp(t)
	h := a.UI()
	w := loginRequest(h, `{"username":"admin","password":"a-strong-test-password"}`, "127.0.0.1:1234")
	cookie := w.Result().Cookies()[0]
	request := func(method, path, body string, want int) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://localhost"+path, strings.NewReader(body))
		r.AddCookie(cookie)
		r.Header.Set("X-Maxstore-Request", "1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w
	}
	request("POST", "/api/buckets", `{"name":"my-files","provider":"local"}`, 201)
	w = request("POST", "/api/uploads", `{"bucket":"my-files","key":"test","size":4}`, 201)
	var u struct {
		ID string `json:"id"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &u); e != nil {
		t.Fatal(e)
	}
	request("PUT", "/api/uploads/"+u.ID+"/parts/1", "data", 200)
	request("POST", "/api/uploads/"+u.ID+"/complete", "", 200)
	w = request("GET", "/api/objects?bucket=my-files&key=test", "", 200)
	if w.Body.String() != "data" {
		t.Fatal("resumable console body mismatch")
	}
}

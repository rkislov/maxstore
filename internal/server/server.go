package server

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"maxstore/internal/store"
)

type Server struct {
	Store                                                    *store.Store
	Token, AccessKey, SecretKey, DefaultProvider, S3Endpoint string
	Assets                                                   embed.FS
	Auth                                                     *ConsoleAuth
}

func jsonReply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func errorStatus(err error) int {
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrUpload):
		return 404
	case errors.Is(err, store.ErrPrecondition):
		return 412
	case errors.Is(err, store.ErrDigest):
		return 400
	case errors.Is(err, store.ErrConflict):
		return 409
	case errors.Is(err, store.ErrInvalid), errors.Is(err, store.ErrPart):
		return 400
	case errors.Is(err, store.ErrTooLarge):
		return 413
	default:
		return 502
	}
}
func apiError(w http.ResponseWriter, e error) {
	slog.Warn("request failed", "error", e)
	jsonReply(w, errorStatus(e), map[string]string{"error": e.Error()})
}
func (s *Server) UI() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(s.Assets)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { jsonReply(w, 200, map[string]string{"status": "ok"}) })
	if s.Auth != nil {
		mux.HandleFunc("/api/auth/", s.Auth.handle)
	}
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		bearer := strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") && subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(s.Token)) == 1
		if !bearer {
			allowed := false
			if s.Auth != nil {
				_, allowed = s.Auth.session(r)
			}
			if !allowed {
				jsonReply(w, 401, map[string]string{"error": "Войдите в консоль"})
				return
			}
			if r.Method != "GET" && r.Method != "HEAD" && (r.Header.Get("X-Maxstore-Request") != "1" || !sameOrigin(r)) {
				jsonReply(w, 403, map[string]string{"error": "Недопустимый запрос"})
				return
			}
		}
		s.api(w, r)
	}))
	return secure(mux)
}
func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	p := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/"), "/")
	if p == "status" && r.Method == "GET" {
		b, e := s.Store.Buckets()
		if e != nil {
			apiError(w, e)
			return
		}
		var count int
		var size int64
		activeUploads, activeDownloads := s.Store.ActiveTransfers()
		for _, v := range b {
			objects, e := s.Store.Objects(v.Name, "")
			if e != nil {
				apiError(w, e)
				return
			}
			count += len(objects)
			for _, o := range objects {
				size += o.Size
			}
		}
		jsonReply(w, 200, map[string]any{"buckets": b, "objects": count, "bytes": size, "providers": s.Store.ProviderNames(), "pending_gc": s.Store.PendingGC(), "chunk_size": store.ChunkSize, "max_size": s.Store.MaxSize, "upload_concurrency": s.Store.UploadConcurrency, "download_concurrency": s.Store.DownloadConcurrency, "active_uploads": activeUploads, "active_downloads": activeDownloads, "s3_endpoint": s.S3Endpoint, "default_provider": s.DefaultProvider})
		return
	}
	if p == "buckets" && r.Method == "POST" {
		var v struct{ Name, Provider string }
		if e := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&v); e != nil {
			jsonReply(w, 400, map[string]string{"error": "Некорректный JSON"})
			return
		}
		if e := s.Store.CreateBucket(v.Name, v.Provider); e != nil {
			apiError(w, e)
			return
		}
		jsonReply(w, 201, v)
		return
	}
	if strings.HasPrefix(p, "buckets/") {
		bucket := strings.TrimPrefix(p, "buckets/")
		switch r.Method {
		case "GET":
			objects, e := s.Store.Objects(bucket, r.URL.Query().Get("prefix"))
			if e != nil {
				apiError(w, e)
				return
			}
			jsonReply(w, 200, objects)
			return
		case "DELETE":
			if e := s.Store.DeleteBucket(bucket); e != nil {
				apiError(w, e)
				return
			}
			w.WriteHeader(204)
			return
		}
	}
	if p == "uploads" || strings.HasPrefix(p, "uploads/") {
		s.uploadAPI(w, r, p)
		return
	}
	if p == "objects" {
		b, k := r.URL.Query().Get("bucket"), r.URL.Query().Get("key")
		switch r.Method {
		case "PUT":
			if s.Store.MaxSize > 0 {
				r.Body = http.MaxBytesReader(w, r.Body, s.Store.MaxSize+1)
			}
			o, e := s.Store.Put(r.Context(), b, k, r.Header.Get("Content-Type"), r.Body, "")
			if e != nil {
				apiError(w, e)
				return
			}
			jsonReply(w, 201, o)
			return
		case "GET", "HEAD":
			s.serveObject(w, r, b, k, true)
			return
		case "DELETE":
			if _, e := s.Store.Bucket(b); e != nil {
				apiError(w, e)
				return
			}
			if e := s.Store.Delete(b, k); e != nil {
				apiError(w, e)
				return
			}
			w.WriteHeader(204)
			return
		}
	}
	jsonReply(w, 404, map[string]string{"error": "Маршрут не найден"})
}
func (s *Server) serveObject(w http.ResponseWriter, r *http.Request, b, k string, api bool) {
	report := func(e error) {
		if api {
			apiError(w, e)
		} else {
			s3Failure(w, e)
		}
	}

	f, o, e := s.Store.OpenReader(r.Context(), b, k)
	if e != nil {
		report(e)
		return
	}
	defer f.Close()
	if value := r.Header.Get("Range"); strings.HasPrefix(value, "bytes=") {
		if strings.Contains(value, ",") {
			f.SingleChunkPrefetch()
		} else {
			bounds := strings.SplitN(strings.TrimPrefix(value, "bytes="), "-", 2)
			if len(bounds) == 2 && bounds[0] != "" && bounds[1] != "" {
				if end, err := strconv.ParseInt(bounds[1], 10, 64); err == nil && end >= 0 {
					f.PrefetchThrough(end)
				}
			}
		}
	}
	objectHeaders(w, o)
	if !api {
		for query, header := range map[string]string{"response-content-type": "Content-Type", "response-content-language": "Content-Language", "response-content-disposition": "Content-Disposition", "response-content-encoding": "Content-Encoding", "response-cache-control": "Cache-Control", "response-expires": "Expires"} {
			if value := r.URL.Query().Get(query); value != "" {
				w.Header().Set(header, value)
			}
		}
	}
	if api {
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": k}))
	}
	http.ServeContent(w, r, k, o.Created, f)
	if f.Err != nil {
		slog.Warn("object stream failed", "error", f.Err)
		panic(http.ErrAbortHandler)
	}
}

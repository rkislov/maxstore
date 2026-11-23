package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"maxstore/internal/provider"
	"maxstore/internal/server"
	"maxstore/internal/store"
	"maxstore/web"
)

func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

type credentials struct {
	ConsoleToken  string `json:"console_token"`
	AccessKey     string `json:"s3_access_key"`
	SecretKey     string `json:"s3_secret_key"`
	AdminUsername string `json:"admin_username,omitempty"`
	AdminPassword string `json:"admin_password,omitempty"`
}

func run() error {
	dir := env("DATA_DIR", "data")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	path := filepath.Join(dir, "credentials.json")
	var creds credentials
	raw, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		creds = credentials{ConsoleToken: provider.ID() + provider.ID(), AccessKey: "ms_" + provider.ID()[:16], SecretKey: provider.ID() + provider.ID()}
		b, _ := json.MarshalIndent(creds, "", "  ")
		if e = os.WriteFile(path, b, 0600); e != nil {
			return e
		}
	} else if e != nil {
		return e
	} else if e = json.Unmarshal(raw, &creds); e != nil {
		return e
	}
	if creds.AdminUsername == "" || creds.AdminPassword == "" {
		creds.AdminUsername = "admin"
		creds.AdminPassword = provider.ID()
		b, err := json.MarshalIndent(creds, "", "  ")
		if err != nil {
			return err
		}
		if err = os.WriteFile(path, b, 0600); err != nil {
			return err
		}
	}
	creds.ConsoleToken = env("CONSOLE_TOKEN", creds.ConsoleToken)
	creds.AccessKey = env("S3_ACCESS_KEY", creds.AccessKey)
	creds.SecretKey = env("S3_SECRET_KEY", creds.SecretKey)
	if len(creds.ConsoleToken) < 16 || len(creds.SecretKey) < 16 || creds.AccessKey == "" {
		return errors.New("console token and S3 secret must have at least 16 characters; access key must not be empty")
	}
	providers := map[string]provider.Provider{"local": &provider.Local{Dir: filepath.Join(dir, "chunks")}}
	if token := os.Getenv("TELEGRAM_BOT_TOKEN"); token != "" {
		chat := os.Getenv("TELEGRAM_CHAT_ID")
		if chat == "" {
			return errors.New("TELEGRAM_CHAT_ID is required")
		}
		providers["telegram"] = &provider.Telegram{Token: token, Chat: chat}
	}
	if token := os.Getenv("MAX_BOT_TOKEN"); token != "" {
		chat := os.Getenv("MAX_CHAT_ID")
		if chat == "" {
			return errors.New("MAX_CHAT_ID is required")
		}
		providers["max"] = &provider.Max{Token: token, Chat: chat}
	}
	defaultProvider := env("DEFAULT_PROVIDER", "local")
	if providers[defaultProvider] == nil {
		return errors.New("DEFAULT_PROVIDER is not configured")
	}
	max, e := strconv.ParseInt(env("MAX_OBJECT_SIZE", "0"), 10, 64)
	if e != nil || max < 0 || max > (1<<63)-2 {
		return errors.New("MAX_OBJECT_SIZE must be 0 (unlimited) or a positive byte count")
	}
	st, e := store.Open(dir, providers, max)
	if e != nil {
		return e
	}
	defer st.Close()
	uploadConcurrency, err := strconv.Atoi(env("UPLOAD_CONCURRENCY", "4"))
	if err != nil {
		return err
	}
	downloadConcurrency, err := strconv.Atoi(env("DOWNLOAD_CONCURRENCY", "4"))
	if err != nil {
		return err
	}
	if err = st.ConfigureConcurrency(uploadConcurrency, downloadConcurrency); err != nil {
		return err
	}
	uiAddr := env("HTTP_ADDR", "127.0.0.1:8080")
	s3Addr := env("S3_ADDR", "127.0.0.1:9000")
	auth, e := server.NewConsoleAuth(st.DB, env("ADMIN_USERNAME", creds.AdminUsername), env("ADMIN_PASSWORD", creds.AdminPassword), os.Getenv("COOKIE_SECURE") == "true")
	if e != nil {
		return e
	}
	app := &server.Server{Auth: auth, Store: st, Token: creds.ConsoleToken, AccessKey: creds.AccessKey, SecretKey: creds.SecretKey, DefaultProvider: defaultProvider, S3Endpoint: env("S3_PUBLIC_ENDPOINT", "http://127.0.0.1:9000"), Assets: web.Assets}
	ui := &http.Server{Addr: uiAddr, Handler: app.UI(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	s3 := &http.Server{Addr: s3Addr, Handler: app.S3(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	ul, e := net.Listen("tcp", uiAddr)
	if e != nil {
		return e
	}
	sl, e := net.Listen("tcp", s3Addr)
	if e != nil {
		ul.Close()
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 2)
	go func() { errc <- ui.Serve(ul) }()
	go func() { errc <- s3.Serve(sl) }()
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			if e := st.ExpireUploads(time.Now()); e != nil && ctx.Err() == nil {
				slog.Warn("Upload expiry failed", "error", e)
			}
			if e := st.Collect(ctx); e != nil && ctx.Err() == nil {
				slog.Warn("Some chunks remain in cleanup queue", "error", e)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	slog.Info("MaxStore started", "console", "http://"+uiAddr, "s3", "http://"+s3Addr, "credentials_file", path, "providers", st.ProviderNames())
	select {
	case <-ctx.Done():
	case e = <-errc:
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = ui.Shutdown(shutdown)
	_ = s3.Shutdown(shutdown)
	_ = ui.Close()
	_ = s3.Close()
	<-gcDone
	if e != nil && !errors.Is(e, http.ErrServerClosed) {
		return e
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "maxstore:", e)
		os.Exit(1)
	}
}

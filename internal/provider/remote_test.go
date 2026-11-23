package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTelegramAdapter(t *testing.T) {
	const token = "secret-token"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bot" + token + "/sendDocument":
			if r.URL.Query().Get("chat_id") != "-100123" {
				t.Error("missing chat")
			}
			if e := r.ParseMultipartForm(1 << 20); e != nil {
				t.Error(e)
			}
			f, _, e := r.FormFile("document")
			if e != nil {
				t.Error(e)
			} else {
				f.Close()
			}
			fmt.Fprint(w, `{"ok":true,"result":{"message_id":42,"document":{"file_id":"file-id"}}}`)
		case "/bot" + token + "/getFile":
			fmt.Fprint(w, `{"ok":true,"result":{"file_path":"docs/test.txt"}}`)
		case "/file/bot" + token + "/docs/test.txt":
			fmt.Fprint(w, "encrypted")
		case "/bot" + token + "/deleteMessage":
			fmt.Fprint(w, `{"ok":true}`)
		default:
			t.Error("unexpected path")
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	old := Client
	Client = srv.Client()
	defer func() { Client = old }()
	p := &Telegram{Token: token, Chat: "-100123", Base: srv.URL}
	ref, e := p.Put(context.Background(), []byte("encrypted"))
	if e != nil || ref.Message != "42" {
		t.Fatalf("put: %+v %v", ref, e)
	}
	data, e := p.Get(context.Background(), ref)
	if e != nil || string(data) != "encrypted" {
		t.Fatalf("get: %s %v", data, e)
	}
	if e = p.Delete(context.Background(), ref); e != nil {
		t.Fatal(e)
	}
}
func TestMaxAdapter(t *testing.T) {
	var base string
	var sends int
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/upload" && r.URL.Path != "/download" && r.Header.Get("Authorization") != "max-token" {
			t.Error("missing authorization")
		}
		switch r.URL.Path {
		case "/uploads":
			fmt.Fprintf(w, `{"url":%q}`, base+"/upload")
		case "/upload":
			if e := r.ParseMultipartForm(1 << 20); e != nil {
				t.Error(e)
			}
			fmt.Fprint(w, `{"token":"attachment-token"}`)
		case "/messages":
			if r.Method == "POST" {
				sends++
				if sends == 1 {
					w.WriteHeader(400)
					fmt.Fprint(w, `{"code":"attachment.not.ready"}`)
					return
				}
				fmt.Fprint(w, `{"message":{"body":{"mid":"message-id"}}}`)
			} else {
				fmt.Fprint(w, `{"success":true}`)
			}
		case "/messages/message-id":
			fmt.Fprintf(w, `{"body":{"mid":"message-id","attachments":[{"type":"file","payload":{"url":%q}}]}}`, base+"/download")
		case "/download":
			fmt.Fprint(w, "encrypted")
		default:
			t.Error("unexpected path")
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	base = srv.URL
	old := Client
	Client = srv.Client()
	defer func() { Client = old }()
	p := &Max{Token: "max-token", Chat: "123", Base: base}
	ref, e := p.Put(context.Background(), []byte("encrypted"))
	if e != nil || ref.Message != "message-id" || sends != 2 {
		t.Fatalf("put: %+v %v sends %d", ref, e, sends)
	}
	data, e := p.Get(context.Background(), ref)
	if e != nil || string(data) != "encrypted" {
		t.Fatalf("get: %s %v", data, e)
	}
	if e = p.Delete(context.Background(), ref); e != nil {
		t.Fatal(e)
	}
}

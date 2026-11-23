package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Client = &http.Client{Timeout: 2 * time.Minute}

type RateLimitError struct{ After time.Duration }

func (e *RateLimitError) Error() string { return "provider rate limited the request" }

// Do not include upstream response bodies or request URLs in errors: they may contain bot tokens.
func request(ctx context.Context, method, endpoint, token, ctype string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return errors.New("invalid provider URL")
	}
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	res, err := Client.Do(req)
	if err != nil {
		return errors.New("provider request failed (network or timeout)")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		if res.StatusCode == http.StatusTooManyRequests {
			var detail struct {
				Parameters struct {
					After int `json:"retry_after"`
				} `json:"parameters"`
			}
			_ = json.NewDecoder(io.LimitReader(res.Body, 8192)).Decode(&detail)
			seconds := detail.Parameters.After
			if seconds < 1 {
				seconds = 1
			}
			if seconds > 300 {
				seconds = 300
			}
			return &RateLimitError{After: time.Duration(seconds) * time.Second}
		}
		return fmt.Errorf("provider HTTP %d", res.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(io.LimitReader(res.Body, 2<<20)).Decode(out); err != nil {
			return errors.New("invalid provider response")
		}
	}
	return nil
}
func form(field string, b []byte) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	p, e := w.CreateFormFile(field, ID()+".txt")
	if e != nil {
		return nil, "", e
	}
	if _, e = p.Write(b); e != nil {
		return nil, "", e
	}
	e = w.Close()
	return &buf, w.FormDataContentType(), e
}
func download(ctx context.Context, u string) ([]byte, error) {
	parsed, err := url.Parse(u)
	if err != nil || parsed.Scheme != "https" {
		return nil, errors.New("provider returned an invalid download URL")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, errors.New("invalid provider download URL")
	}
	res, err := Client.Do(req)
	if err != nil {
		return nil, errors.New("provider download failed")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("download HTTP %d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, (9<<20)+1))
	if len(b) > 9<<20 {
		return nil, errors.New("provider chunk exceeds size limit")
	}
	return b, err
}

type Telegram struct {
	Token, Chat, Base string
	mu                sync.Mutex
	nextSend          time.Time
}

func (t *Telegram) waitSend(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if wait := time.Until(t.nextSend); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	interval := 1100 * time.Millisecond
	if strings.HasPrefix(t.Chat, "-") {
		interval = 3100 * time.Millisecond
	}
	t.nextSend = time.Now().Add(interval)
	return nil
}

func (t *Telegram) base() string {
	if t.Base != "" {
		return strings.TrimRight(t.Base, "/")
	}
	return "https://api.telegram.org"
}
func (t *Telegram) call(ctx context.Context, method string, v any, out any) error {
	b, _ := json.Marshal(v)
	return request(ctx, "POST", t.base()+"/bot"+t.Token+"/"+method, "", "application/json", bytes.NewReader(b), out)
}
func (t *Telegram) Put(ctx context.Context, b []byte) (Ref, error) {
	if e := t.waitSend(ctx); e != nil {
		return Ref{}, e
	}
	buf, ct, e := form("document", b)
	if e != nil {
		return Ref{}, e
	}
	var res struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int64 `json:"message_id"`
			Document  struct {
				FileID string `json:"file_id"`
			} `json:"document"`
		} `json:"result"`
	}
	for attempt := 0; attempt < 5; attempt++ {
		e = request(ctx, "POST", t.base()+"/bot"+t.Token+"/sendDocument?chat_id="+url.QueryEscape(t.Chat), "", ct, bytes.NewReader(buf.Bytes()), &res)
		var limited *RateLimitError
		if !errors.As(e, &limited) {
			break
		}
		timer := time.NewTimer(limited.After)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Ref{}, ctx.Err()
		case <-timer.C:
		}
	}
	if e != nil {
		return Ref{}, e
	}
	if !res.OK || res.Result.Document.FileID == "" {
		return Ref{}, errors.New("Telegram did not return a file identifier")
	}
	return Ref{ID: res.Result.Document.FileID, Message: strconv.FormatInt(res.Result.MessageID, 10)}, nil
}
func (t *Telegram) Get(ctx context.Context, r Ref) ([]byte, error) {
	var res struct {
		OK     bool `json:"ok"`
		Result struct {
			Path string `json:"file_path"`
		} `json:"result"`
	}
	if e := t.call(ctx, "getFile", map[string]string{"file_id": r.ID}, &res); e != nil {
		return nil, e
	}
	if !res.OK || res.Result.Path == "" {
		return nil, errors.New("Telegram file unavailable")
	}
	return download(ctx, t.base()+"/file/bot"+t.Token+"/"+res.Result.Path)
}
func (t *Telegram) Delete(ctx context.Context, r Ref) error {
	id, e := strconv.ParseInt(r.Message, 10, 64)
	if e != nil {
		return e
	}
	var res struct {
		OK bool `json:"ok"`
	}
	if e = t.call(ctx, "deleteMessage", map[string]any{"chat_id": t.Chat, "message_id": id}, &res); e != nil {
		return e
	}
	if !res.OK {
		return errors.New("Telegram refused message deletion")
	}
	return nil
}

type Max struct{ Token, Chat, Base string }

func (m *Max) base() string {
	if m.Base != "" {
		return strings.TrimRight(m.Base, "/")
	}
	return "https://platform-api2.max.ru"
}

type maxMessage struct {
	Body struct {
		MID         string `json:"mid"`
		Attachments []struct {
			Type    string `json:"type"`
			Payload struct {
				URL string `json:"url"`
			} `json:"payload"`
		} `json:"attachments"`
	} `json:"body"`
}

func (m *Max) Put(ctx context.Context, b []byte) (Ref, error) {
	var up struct {
		URL string `json:"url"`
	}
	if e := request(ctx, "POST", m.base()+"/uploads?type=file", m.Token, "", nil, &up); e != nil {
		return Ref{}, e
	}
	u, e := url.Parse(up.URL)
	if e != nil || u.Scheme != "https" {
		return Ref{}, errors.New("MAX returned invalid upload URL")
	}
	buf, ct, e := form("data", b)
	if e != nil {
		return Ref{}, e
	}
	var uploaded struct {
		Token string `json:"token"`
	}
	if e = request(ctx, "POST", up.URL, "", ct, buf, &uploaded); e != nil {
		return Ref{}, e
	}
	if uploaded.Token == "" {
		return Ref{}, errors.New("MAX did not return an attachment token")
	}
	payload, _ := json.Marshal(map[string]any{"text": "MaxStore encrypted chunk", "attachments": []any{map[string]any{"type": "file", "payload": map[string]string{"token": uploaded.Token}}}})
	var res struct {
		Message maxMessage `json:"message"`
	}
	// Attachment processing is asynchronous. Retry only the documented not-ready response.
	for attempt := 0; attempt < 6; attempt++ {
		e = m.send(ctx, payload, &res)
		if e == nil {
			if res.Message.Body.MID == "" {
				return Ref{}, errors.New("MAX did not return a message identifier")
			}
			return Ref{ID: uploaded.Token, Message: res.Message.Body.MID}, nil
		}
		if !errors.Is(e, errNotReady) {
			return Ref{}, e
		}
		select {
		case <-ctx.Done():
			return Ref{}, ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
	}
	return Ref{}, e
}

var errNotReady = errors.New("MAX attachment is still processing")

func (m *Max) send(ctx context.Context, payload []byte, out any) error {
	req, e := http.NewRequestWithContext(ctx, "POST", m.base()+"/messages?chat_id="+url.QueryEscape(m.Chat), bytes.NewReader(payload))
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", m.Token)
	req.Header.Set("Content-Type", "application/json")
	res, e := Client.Do(req)
	if e != nil {
		return errors.New("MAX send request failed")
	}
	defer res.Body.Close()
	b, e := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if e != nil {
		return e
	}
	if res.StatusCode >= 300 {
		var v struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(b, &v)
		if v.Code == "attachment.not.ready" {
			return errNotReady
		}
		return fmt.Errorf("MAX send HTTP %d", res.StatusCode)
	}
	return json.Unmarshal(b, out)
}
func (m *Max) Get(ctx context.Context, r Ref) ([]byte, error) {
	var msg maxMessage
	if e := request(ctx, "GET", m.base()+"/messages/"+url.PathEscape(r.Message), m.Token, "", nil, &msg); e != nil {
		return nil, e
	}
	for _, a := range msg.Body.Attachments {
		if a.Type == "file" && a.Payload.URL != "" {
			return download(ctx, a.Payload.URL)
		}
	}
	return nil, errors.New("MAX attachment unavailable")
}
func (m *Max) Delete(ctx context.Context, r Ref) error {
	var res struct {
		Success bool `json:"success"`
	}
	if e := request(ctx, "DELETE", m.base()+"/messages?message_id="+url.QueryEscape(r.Message), m.Token, "", nil, &res); e != nil {
		return e
	}
	if !res.Success {
		return errors.New("MAX refused message deletion")
	}
	return nil
}

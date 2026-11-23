package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func hash(s string) string { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
func mac(key []byte, s string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(s))
	return h.Sum(nil)
}
func encode(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
func canonicalQuery(v url.Values) string {
	var entries []string
	for k, values := range v {
		if k == "X-Amz-Signature" {
			continue
		}
		for _, v := range values {
			entries = append(entries, encode(k)+"="+encode(v))
		}
	}
	sort.Strings(entries)
	return strings.Join(entries, "&")
}
func (s *Server) verify(r *http.Request) error {
	q := r.URL.Query()
	pre := q.Get("X-Amz-Algorithm") != ""
	var credential, signed, sig, date, payload string
	if pre {
		if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
			return errors.New("unsupported signature")
		}
		credential = q.Get("X-Amz-Credential")
		signed = q.Get("X-Amz-SignedHeaders")
		sig = q.Get("X-Amz-Signature")
		date = q.Get("X-Amz-Date")
		payload = "UNSIGNED-PAYLOAD"
	} else {
		a := strings.TrimPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ")
		if a == r.Header.Get("Authorization") {
			return errors.New("signature required")
		}
		parts := map[string]string{}
		for _, p := range strings.Split(a, ",") {
			kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
			if len(kv) == 2 {
				parts[kv[0]] = kv[1]
			}
		}
		credential = parts["Credential"]
		signed = parts["SignedHeaders"]
		sig = parts["Signature"]
		date = r.Header.Get("X-Amz-Date")
		payload = r.Header.Get("X-Amz-Content-Sha256")
		if payload == "" {
			return errors.New("payload hash required")
		}
	}
	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[0] != s.AccessKey || scope[3] != "s3" || scope[4] != "aws4_request" {
		return errors.New("invalid credential scope")
	}
	t, e := time.Parse("20060102T150405Z", date)
	if e != nil || scope[1] != t.Format("20060102") {
		return errors.New("invalid date")
	}
	now := time.Now()
	if pre {
		seconds, e := strconv.Atoi(q.Get("X-Amz-Expires"))
		if e != nil || seconds < 1 || seconds > 604800 || now.After(t.Add(time.Duration(seconds)*time.Second)) || t.After(now.Add(15*time.Minute)) {
			return errors.New("expired signature")
		}
	} else if now.Sub(t) > 15*time.Minute || t.Sub(now) > 15*time.Minute {
		return errors.New("expired signature")
	}
	headers := strings.Split(signed, ";")
	if signed == "" || !sort.StringsAreSorted(headers) {
		return errors.New("invalid signed headers")
	}
	hasHost := false
	var canonical strings.Builder
	for i, h := range headers {
		if h == "" || h != strings.ToLower(h) || (i > 0 && headers[i-1] == h) {
			return errors.New("invalid signed header")
		}
		values := r.Header.Values(h)
		for j := range values {
			values[j] = strings.Join(strings.Fields(values[j]), " ")
		}
		v := strings.Join(values, ",")
		if h == "content-length" {
			v = strconv.FormatInt(r.ContentLength, 10)
		}
		if h == "host" {
			v = r.Host
			hasHost = true
		}
		canonical.WriteString(h + ":" + strings.Join(strings.Fields(v), " ") + "\n")
	}
	if !hasHost {
		return errors.New("host must be signed")
	}
	if r.Header.Get("X-Maxstore-Provider") != "" && !strings.Contains(";"+signed+";", ";x-maxstore-provider;") {
		return errors.New("provider selection header must be signed")
	}
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	segments := strings.Split(path, "/")
	for i := range segments {
		segments[i] = encode(segments[i])
	}
	uri := strings.Join(segments, "/")
	cr := strings.Join([]string{r.Method, uri, canonicalQuery(q), canonical.String(), signed, payload}, "\n")
	sts := "AWS4-HMAC-SHA256\n" + date + "\n" + strings.Join(scope[1:], "/") + "\n" + hash(cr)
	k := mac([]byte("AWS4"+s.SecretKey), scope[1])
	k = mac(k, scope[2])
	k = mac(k, "s3")
	k = mac(k, "aws4_request")
	expected := mac(k, sts)
	got, e := hex.DecodeString(sig)
	if e != nil || !hmac.Equal(expected, got) {
		return errors.New("signature mismatch")
	}
	return nil
}

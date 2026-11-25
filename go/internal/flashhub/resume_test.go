package flashhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aodianyun/flashcli/go/internal/progress"
)

func TestDownloadFileResumesVerifiedRangeWithinCall(t *testing.T) {
	payload := []byte("0123456789abcdef")
	const etag = `"bundle-v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
				t.Errorf("initial request has range headers: %+v", r.Header)
			}
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:5])
			sent.Add(5)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if got := r.Header.Get("Range"); got != "bytes=5-" {
				t.Errorf("Range = %q", got)
			}
			if got := r.Header.Get("If-Range"); got != etag {
				t.Errorf("If-Range = %q", got)
			}
			if got := r.Header.Get("Accept-Encoding"); got != "identity" {
				t.Errorf("Accept-Encoding = %q", got)
			}
			suffix := payload[5:]
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 5-%d/%d", len(payload)-1, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprint(len(suffix)))
			w.WriteHeader(http.StatusPartialContent)
			n, _ := w.Write(suffix)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request %d", requests.Load())
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("downloadFileWithResume: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("destination=%q err=%v", got, err)
	}
	if requests.Load() != 2 || sent.Load() != int64(len(payload)) {
		t.Fatalf("requests=%d response bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileRangeIgnoredRestartsOwnedTemporaryFile(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:3])
			sent.Add(3)
			_ = rw.Flush()
			_ = conn.Close()
			return
		}
		if r.Header.Get("Range") != "bytes=3-" {
			t.Errorf("Range=%q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		n, _ := w.Write(payload)
		sent.Add(int64(n))
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 2 || sent.Load() != 13 {
		t.Fatalf("requests=%d bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileRejectsBadRangeAndFallsBackWithinAttemptBudget(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var rangeBytes, fallbackBytes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:4])
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=4-" || r.Header.Get("If-Range") != etag {
				t.Errorf("range headers=%+v", r.Header)
			}
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Range", "bytes 0-5/10")
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusPartialContent)
			n, _ := io.WriteString(w, "xxxxxx")
			rangeBytes.Add(int64(n))
		case 3:
			if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
				t.Errorf("fallback headers=%+v", r.Header)
			}
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			_, _ = w.Write(payload)
			fallbackBytes.Add(int64(len(payload)))
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 3 || rangeBytes.Load() != 6 || fallbackBytes.Load() != 10 {
		t.Fatalf("requests=%d range response bytes=%d fallback bytes=%d", requests.Load(), rangeBytes.Load(), fallbackBytes.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileDoesNotRangeResumeWithoutStrongETagOrKnownTotal(t *testing.T) {
	for _, tc := range []struct {
		name, etag string
		size       int64
	}{{name: "missing-etag", size: 10}, {name: "weak-etag", etag: `W/"v1"`, size: 10}, {name: "unknown-total", etag: `"v1"`}} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte("abcdefghij")
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					conn, rw, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("hijack: %v", err)
						return
					}
					if tc.size > 0 {
						_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n", len(payload))
					} else {
						_, _ = io.WriteString(rw, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n")
					}
					if tc.etag != "" {
						_, _ = fmt.Fprintf(rw, "ETag: %s\r\n", tc.etag)
					}
					_, _ = io.WriteString(rw, "\r\n")
					if tc.size > 0 {
						_, _ = rw.Write(payload[:4])
					} else {
						_, _ = io.WriteString(rw, "4\r\nabcd\r\n")
					}
					_ = rw.Flush()
					_ = conn.Close()
					return
				}
				if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
					t.Errorf("ineligible range headers=%+v", r.Header)
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
				w.Header().Set("ETag", `"v1"`)
				_, _ = w.Write(payload)
			}))
			defer srv.Close()
			dest := filepath.Join(t.TempDir(), "model.bin")
			err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: tc.size}, dest, true, true, nil, nil, true)
			if err != nil {
				t.Fatalf("download: %v", err)
			}
			if requests.Load() != 2 {
				t.Fatalf("requests=%d", requests.Load())
			}
			if _, err := os.Stat(dest); err != nil {
				t.Fatalf("destination missing: %v", err)
			}
			assertNoDownloadTemps(t, dest)
		})
	}
}

func TestDownloadFileRangeCancellationLeavesNoTemporaryFiles(t *testing.T) {
	payload := []byte(strings.Repeat("x", 64))
	started := make(chan struct{})
	allowClose := make(chan struct{})
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), `"v1"`)
			_, _ = rw.Write(payload[:8])
			_ = rw.Flush()
			close(started)
			<-allowClose
			_ = conn.Close()
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	dest := filepath.Join(t.TempDir(), "model.bin")
	result := make(chan error, 1)
	go func() {
		result <- testDownloadClient(srv.Client()).downloadFileWithResume(ctx, File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload))}, dest, true, true, nil, nil, true)
	}()
	<-started
	close(allowClose)
	cancel()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("download did not cancel")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destination stat=%v", err)
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileRange416WithMismatchedOffsetFallsBack(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:4])
			sent.Add(4)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=4-" || r.Header.Get("If-Range") != etag {
				t.Errorf("range headers=%+v", r.Header)
			}
			w.Header().Set("Content-Range", "bytes */10")
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		case 3:
			if r.Header.Get("Range") != "" {
				t.Errorf("fallback Range=%q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			w.Header().Set("ETag", etag)
			n, _ := w.Write(payload)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 3 || sent.Load() != 14 {
		t.Fatalf("requests=%d server body bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileRangeUnsupportedFallsBackWithoutPrefix(t *testing.T) {
	payload := []byte("range-is-unsupported")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			if r.Header.Get("Range") != "" {
				t.Errorf("initial request unexpectedly has Range=%q", r.Header.Get("Range"))
			}
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:5])
			sent.Add(5)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=5-" || r.Header.Get("If-Range") != etag {
				t.Errorf("range request headers=%+v", r.Header)
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
		case 3:
			if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
				t.Errorf("full fallback headers=%+v", r.Header)
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			n, _ := w.Write(payload)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request %d", requests.Load())
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("destination=%q err=%v", got, err)
	}
	if requests.Load() != 3 || sent.Load() != int64(5+len(payload)) {
		t.Fatalf("requests=%d response body bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileChangedETagOn206FallsBackWithoutAppending(t *testing.T) {
	payload := []byte("abcdefghij")
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), `"old"`)
			_, _ = rw.Write(payload[:4])
			sent.Add(4)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("If-Range") != `"old"` {
				t.Errorf("If-Range=%q", r.Header.Get("If-Range"))
			}
			w.Header().Set("ETag", `"new"`)
			w.Header().Set("Content-Range", "bytes 4-9/10")
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusPartialContent)
			n, _ := w.Write([]byte("XXXXXX"))
			sent.Add(int64(n))
		case 3:
			if r.Header.Get("Range") != "" {
				t.Errorf("fallback Range=%q", r.Header.Get("Range"))
			}
			w.Header().Set("ETag", `"new"`)
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			n, _ := w.Write(payload)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 3 || sent.Load() != 20 {
		t.Fatalf("requests=%d server body bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileRangeRedirectDoesNotForwardIfRange(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		t.Errorf("range redirect was followed; headers=%+v", r.Header)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:4])
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=4-" || r.Header.Get("If-Range") != etag {
				t.Errorf("range headers=%+v", r.Header)
			}
			http.Redirect(w, r, target.URL, http.StatusFound)
		case 3:
			if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
				t.Errorf("full fallback headers=%+v", r.Header)
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			w.Header().Set("ETag", etag)
			_, _ = w.Write(payload)
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if redirected.Load() != 0 || requests.Load() != 3 {
		t.Fatalf("redirected requests=%d source requests=%d", redirected.Load(), requests.Load())
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileResumesAfterMultipleInterruptionsOnThirdRequest(t *testing.T) {
	payload := []byte("abcdefghijkl")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	var offsets []string
	var reported []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:3])
			sent.Add(3)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			offsets = append(offsets, r.Header.Get("Range"))
			if r.Header.Get("Range") != "bytes=3-" {
				t.Errorf("Range=%q", r.Header.Get("Range"))
			}
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 206 Partial Content\r\nContent-Length: 9\r\nContent-Range: bytes 3-11/12\r\nETag: %s\r\n\r\n", etag)
			_, _ = rw.Write(payload[3:6])
			sent.Add(3)
			_ = rw.Flush()
			_ = conn.Close()
		case 3:
			offsets = append(offsets, r.Header.Get("Range"))
			if r.Header.Get("Range") != "bytes=6-" {
				t.Errorf("Range=%q", r.Header.Get("Range"))
			}
			suffix := payload[6:]
			w.Header().Set("Content-Range", "bytes 6-11/12")
			w.Header().Set("Content-Length", fmt.Sprint(len(suffix)))
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusPartialContent)
			n, _ := w.Write(suffix)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request %d", requests.Load())
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, func(n int64) { reported = append(reported, n) }, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 3 || sent.Load() != int64(len(payload)) {
		t.Fatalf("requests=%d body bytes=%d", requests.Load(), sent.Load())
	}
	if fmt.Sprint(offsets) != "[bytes=3- bytes=6-]" {
		t.Fatalf("range offsets=%v", offsets)
	}
	if len(reported) < 3 || reported[len(reported)-1] != int64(len(payload)) {
		t.Fatalf("reported logical offsets=%v", reported)
	}
	for i := 1; i < len(reported); i++ {
		if reported[i] < reported[i-1] {
			t.Fatalf("progress regressed: %v", reported)
		}
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileResumeExhaustsOriginalThreeRequestBudget(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:2])
			_ = rw.Flush()
			_ = conn.Close()
		case 2, 3:
			if r.Header.Get("Range") == "" {
				t.Errorf("attempt %d had no Range", requests.Load())
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 2-%d/%d", len(payload)-1, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)-2))
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[2:4])
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload))}, dest, true, true, nil, nil, true)
	if err == nil || !strings.Contains(err.Error(), "after 3 attempt(s)") {
		t.Fatalf("error=%v", err)
	}
	if requests.Load() != 3 {
		t.Fatalf("requests=%d", requests.Load())
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destination stat=%v", err)
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileResumeRejectsCompressedResponse(t *testing.T) {
	payload := []byte("plain-bytes")
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload))}, dest, true, true, nil, nil, true)
	if err == nil || !strings.Contains(err.Error(), "compressed response") {
		t.Fatalf("error=%v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests=%d", requests.Load())
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destination stat=%v", err)
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileResumeRepairsDamagedCache(t *testing.T) {
	payload := []byte("fixed-cache")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = w.Write(payload)
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(dest, []byte("old-damaged"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, false, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	assertNoDownloadTemps(t, dest)
}

func TestSyncBundleRangeResumeReportsCumulativeLiveProgress(t *testing.T) {
	manifest := []byte(`{"name":"range-test"}`)
	payload := []byte(strings.Repeat("m", 100))
	const etag = `"bundle-v1"`
	var modelRequests atomic.Int32
	var modelBodyBytes atomic.Int64
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/index":
			index, err := json.Marshal(map[string]any{"code": 0, "data": map[string]any{"files": []any{
				map[string]any{"download_url": srv.URL + "/flashcli-bundle.json", "file_name": "flashcli-bundle.json", "file_size": len(manifest), "md5_hash": payloadMD5(manifest)},
				map[string]any{"download_url": srv.URL + "/model.bin", "file_name": "model.bin", "file_size": len(payload), "md5_hash": payloadMD5(payload)},
			}}})
			if err != nil {
				t.Errorf("marshal index: %v", err)
				return
			}
			_, _ = w.Write(index)
		case "/flashcli-bundle.json":
			w.Header().Set("Content-Length", fmt.Sprint(len(manifest)))
			_, _ = w.Write(manifest)
		case "/model.bin":
			switch modelRequests.Add(1) {
			case 1:
				conn, rw, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
				_, _ = rw.Write(payload[:40])
				modelBodyBytes.Add(40)
				_ = rw.Flush()
				_ = conn.Close()
			case 2:
				if r.Header.Get("Range") != "bytes=40-" || r.Header.Get("If-Range") != etag {
					t.Errorf("range headers=%+v", r.Header)
				}
				suffix := payload[40:]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 40-%d/%d", len(payload)-1, len(payload)))
				w.Header().Set("Content-Length", fmt.Sprint(len(suffix)))
				w.Header().Set("ETag", etag)
				w.WriteHeader(http.StatusPartialContent)
				n, _ := w.Write(suffix)
				modelBodyBytes.Add(int64(n))
			default:
				t.Errorf("unexpected model request")
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	client := NewClient()
	client.HTTP = srv.Client()
	client.CacheDir = filepath.Join(t.TempDir(), "index-cache")
	bundleDir := t.TempDir()
	updates := make(chan progress.Update, 128)
	ctx := progress.WithSink(context.Background(), func(update progress.Update) { updates <- update })
	if _, err := client.SyncBundle(ctx, srv.URL+"/index", bundleDir, "", nil, true); err != nil {
		t.Fatalf("SyncBundle: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(bundleDir, "model.bin"))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("model=%q err=%v", got, err)
	}
	if modelRequests.Load() != 2 || modelBodyBytes.Load() != int64(len(payload)) {
		t.Fatalf("requests=%d body bytes=%d", modelRequests.Load(), modelBodyBytes.Load())
	}
	var last progress.Update
	seenLive := false
	for {
		select {
		case update := <-updates:
			if update.Percent > 100 {
				t.Fatalf("progress exceeded 100%%: %+v", update)
			}
			if update.Total > 0 && update.Written > update.Total {
				t.Fatalf("progress exceeded total: %+v", update)
			}
			if update.Written > int64(len(manifest)) && !update.Done {
				seenLive = true
			}
			last = update
		default:
			if !seenLive {
				t.Fatal("no live model progress received")
			}
			if !last.Done || last.Total <= 0 || last.Written != last.Total || last.Percent != 100 {
				t.Fatalf("final progress=%+v", last)
			}
			return
		}
	}
}

func TestDownloadFileRange503RetryKeepsSameOffsetAndBudget(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:4])
			sent.Add(4)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=4-" || r.Header.Get("If-Range") != etag {
				t.Errorf("retry headers=%+v", r.Header)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		case 3:
			if r.Header.Get("Range") != "bytes=4-" || r.Header.Get("If-Range") != etag {
				t.Errorf("resume headers=%+v", r.Header)
			}
			suffix := payload[4:]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 4-%d/10", len(payload)-1))
			w.Header().Set("Content-Length", fmt.Sprint(len(suffix)))
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusPartialContent)
			n, _ := w.Write(suffix)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 3 || sent.Load() != int64(len(payload)) {
		t.Fatalf("requests=%d body bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileRejectsOverlongChunkedRangeBody(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var rangeBytes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:4])
			_ = rw.Flush()
			_ = conn.Close()
			return
		}
		if r.Header.Get("Range") != "bytes=4-" {
			t.Errorf("Range=%q", r.Header.Get("Range"))
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\nContent-Range: bytes 4-9/10\r\nETag: %s\r\n\r\n", etag)
		_, _ = io.WriteString(rw, "7\r\nXXXXXXX\r\n0\r\n\r\n")
		rangeBytes.Add(7)
		_ = rw.Flush()
		_ = conn.Close()
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload))}, dest, true, true, nil, nil, true)
	if err == nil || !strings.Contains(err.Error(), "exceeds expected length") {
		t.Fatalf("error=%v", err)
	}
	if requests.Load() != 2 || rangeBytes.Load() != 7 {
		t.Fatalf("requests=%d range body bytes=%d", requests.Load(), rangeBytes.Load())
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destination stat=%v", err)
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileAccepts416OnlyForVerifiedCompleteCandidate(t *testing.T) {
	payload := []byte("complete")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nETag: %s\r\n\r\n", etag)
			_, _ = fmt.Fprintf(rw, "%x\r\n", len(payload))
			_, _ = rw.Write(payload)
			_, _ = io.WriteString(rw, "\r\n")
			sent.Add(int64(len(payload)))
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=8-" || r.Header.Get("If-Range") != etag {
				t.Errorf("range headers=%+v", r.Header)
			}
			w.Header().Set("Content-Range", "bytes */8")
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("destination=%q err=%v", got, err)
	}
	if requests.Load() != 2 || sent.Load() != int64(len(payload)) {
		t.Fatalf("requests=%d body bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileConcurrentConflictingMetadataKeepsPR1CommitRule(t *testing.T) {
	payloadA := []byte("version-a")
	payloadB := []byte("version-b")
	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		var payload []byte
		if r.URL.Path == "/a" {
			payload = payloadA
		} else {
			payload = payloadB
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = w.Write(payload)
		requests.Add(1)
	}))
	defer srv.Close()
	client := testDownloadClient(srv.Client())
	dest := filepath.Join(t.TempDir(), "model.bin")
	files := []File{{Path: "model.bin", URL: srv.URL + "/a", Size: int64(len(payloadA)), MD5: payloadMD5(payloadA)}, {Path: "model.bin", URL: srv.URL + "/b", Size: int64(len(payloadB)), MD5: payloadMD5(payloadB)}}
	results := make(chan error, 2)
	for _, file := range files {
		file := file
		go func() {
			results <- client.downloadFileWithResume(context.Background(), file, dest, true, true, nil, nil, true)
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("downloads did not overlap")
		}
	}
	release <- struct{}{}
	release <- struct{}{}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("download: %v", err)
		}
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !((string(got) == string(payloadA) && payloadMD5(got) == files[0].MD5) || (string(got) == string(payloadB) && payloadMD5(got) == files[1].MD5)) {
		t.Fatalf("final file is not one caller's validated version: %q", got)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests=%d", requests.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestSyncBundleMixedUnknownSizeDoesNotReportCompleteEarly(t *testing.T) {
	manifest := []byte(`{"name":"mixed-progress"}`)
	known := []byte("known-file")
	unknown := []byte("unknown-file")
	unknownStarted := make(chan struct{})
	allowUnknownFinish := make(chan struct{})
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/index":
			index, err := json.Marshal(map[string]any{"code": 0, "data": map[string]any{"files": []any{
				map[string]any{"download_url": srv.URL + "/flashcli-bundle.json", "file_name": "flashcli-bundle.json", "file_size": len(manifest), "md5_hash": payloadMD5(manifest)},
				map[string]any{"download_url": srv.URL + "/known.bin", "file_name": "known.bin", "file_size": len(known), "md5_hash": payloadMD5(known)},
				map[string]any{"download_url": srv.URL + "/unknown.bin", "file_name": "unknown.bin", "md5_hash": payloadMD5(unknown)},
			}}})
			if err != nil {
				t.Errorf("marshal index: %v", err)
				return
			}
			_, _ = w.Write(index)
		case "/flashcli-bundle.json":
			_, _ = w.Write(manifest)
		case "/known.bin":
			_, _ = w.Write(known)
		case "/unknown.bin":
			w.Header().Set("Content-Length", fmt.Sprint(len(unknown)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(unknown[:1])
			w.(http.Flusher).Flush()
			close(unknownStarted)
			<-allowUnknownFinish
			_, _ = w.Write(unknown[1:])
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	client := NewClient()
	client.HTTP = srv.Client()
	client.CacheDir = filepath.Join(t.TempDir(), "cache")
	bundleDir := t.TempDir()
	updates := make(chan progress.Update, 128)
	ctx := progress.WithSink(context.Background(), func(update progress.Update) { updates <- update })
	done := make(chan error, 1)
	go func() { _, err := client.SyncBundle(ctx, srv.URL+"/index", bundleDir, "", nil, true); done <- err }()
	select {
	case <-unknownStarted:
	case <-time.After(time.Second):
		t.Fatal("unknown file transfer did not start")
	}
	deadline := time.After(time.Second)
	sawUnknownProgress := false
	for !sawUnknownProgress {
		select {
		case update := <-updates:
			if update.Percent >= 100 && !update.Done {
				t.Fatalf("reported complete before unknown file finished: %+v", update)
			}
			if update.FilesDone == 2 && update.FilesTotal == 3 {
				sawUnknownProgress = true
			}
		case <-deadline:
			t.Fatal("no progress update while unknown-size file was active")
		}
	}
	close(allowUnknownFinish)
	if err := <-done; err != nil {
		t.Fatalf("SyncBundle: %v", err)
	}
	var last progress.Update
	for {
		select {
		case update := <-updates:
			if update.Percent > 100 {
				t.Fatalf("progress exceeded 100%%: %+v", update)
			}
			last = update
		default:
			if !last.Done || last.Percent != 100 || last.FilesDone != 3 {
				t.Fatalf("final progress=%+v", last)
			}
			return
		}
	}
}

func TestDownloadFileRangeContentLengthMismatchFallsBack(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"v1"`
	var requests atomic.Int32
	var sent atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			_, _ = rw.Write(payload[:4])
			sent.Add(4)
			_ = rw.Flush()
			_ = conn.Close()
		case 2:
			if r.Header.Get("Range") != "bytes=4-" {
				t.Errorf("Range=%q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", "bytes 4-9/10")
			w.Header().Set("Content-Length", "5")
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusPartialContent)
			n, _ := w.Write([]byte("XXXXX"))
			sent.Add(int64(n))
		case 3:
			if r.Header.Get("Range") != "" {
				t.Errorf("fallback Range=%q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			w.Header().Set("ETag", etag)
			n, _ := w.Write(payload)
			sent.Add(int64(n))
		default:
			t.Errorf("unexpected request")
		}
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("destination=%q", got)
	}
	if requests.Load() != 3 || sent.Load() != 19 {
		t.Fatalf("requests=%d server body bytes=%d", requests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestParseContentRangeTable(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		wantOK bool
		start  int64
		end    int64
		total  int64
	}{
		{name: "normal", values: []string{"bytes 5-9/10"}, wantOK: true, start: 5, end: 9, total: 10},
		{name: "duplicate-header", values: []string{"bytes 5-9/10", "bytes 5-9/10"}},
		{name: "wrong-unit", values: []string{"items 5-9/10"}},
		{name: "unknown-total", values: []string{"bytes 5-9/*"}},
		{name: "negative-start", values: []string{"bytes -1-9/10"}},
		{name: "negative-total", values: []string{"bytes 5-9/-10"}},
		{name: "integer-overflow", values: []string{"bytes 9223372036854775808-9223372036854775809/9223372036854775810"}},
		{name: "start-after-end", values: []string{"bytes 9-5/10"}},
		{name: "end-equals-total", values: []string{"bytes 5-10/10"}},
		{name: "end-overflows-total", values: []string{"bytes 5-11/10"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start, end, total, ok := parseContentRange(test.values)
			if ok != test.wantOK {
				t.Fatalf("parseContentRange(%q) ok=%v, want %v", test.values, ok, test.wantOK)
			}
			if ok && (start != test.start || end != test.end || total != test.total) {
				t.Fatalf("parseContentRange(%q)=(%d,%d,%d), want (%d,%d,%d)", test.values, start, end, total, test.start, test.end, test.total)
			}
		})
	}
}

func TestDownloadFileRangeOffsetAndBodyLengthTable(t *testing.T) {
	payload := []byte("abcdefghij")
	const etag = `"range-test"`
	tests := []struct {
		name         string
		wantRanges   string
		wantRequests int32
		wantSent     int64
		wantErr      bool
	}{
		{name: "start-does-not-match-local-offset", wantRanges: "bytes=4-,", wantRequests: 3, wantSent: 21},
		{name: "short-response-body-resumes-from-new-offset", wantRanges: "bytes=4-,bytes=6-", wantRequests: 3, wantSent: 10},
		{name: "long-response-body-is-rejected", wantRanges: "bytes=4-", wantRequests: 2, wantSent: 11, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			var sent atomic.Int64
			var ranges []string
			tempDir := t.TempDir()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch requests.Add(1) {
				case 1:
					conn, rw, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("hijack initial response: %v", err)
						return
					}
					_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
					_, _ = rw.Write(payload[:4])
					sent.Add(4)
					_ = rw.Flush()
					_ = conn.Close()
				case 2:
					ranges = append(ranges, r.Header.Get("Range"))
					if r.Header.Get("Range") != "bytes=4-" || r.Header.Get("If-Range") != etag {
						t.Errorf("range headers=%+v", r.Header)
					}
					switch test.name {
					case "start-does-not-match-local-offset":
						w.Header().Set("Content-Range", "bytes 3-9/10")
						w.Header().Set("Content-Length", "7")
						w.Header().Set("ETag", etag)
						w.WriteHeader(http.StatusPartialContent)
						n, _ := io.WriteString(w, "XXXXXXX")
						sent.Add(int64(n))
					case "short-response-body-resumes-from-new-offset":
						conn, rw, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Errorf("hijack short response: %v", err)
							return
						}
						_, _ = fmt.Fprintf(rw, "HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: %s\r\n\r\n", etag)
						_, _ = rw.Write(payload[4:6])
						sent.Add(2)
						_ = rw.Flush()
						_ = conn.Close()
					case "long-response-body-is-rejected":
						conn, rw, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Errorf("hijack long response: %v", err)
							return
						}
						_, _ = fmt.Fprintf(rw, "HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\nContent-Range: bytes 4-9/10\r\nETag: %s\r\n\r\n", etag)
						_, _ = io.WriteString(rw, "7\r\nXXXXXXX\r\n0\r\n\r\n")
						sent.Add(7)
						_ = rw.Flush()
						_ = conn.Close()
					}
				case 3:
					ranges = append(ranges, r.Header.Get("Range"))
					if test.name == "short-response-body-resumes-from-new-offset" {
						if r.Header.Get("Range") != "bytes=6-" {
							t.Errorf("retry Range=%q", r.Header.Get("Range"))
						}
						w.Header().Set("Content-Range", "bytes 6-9/10")
						w.Header().Set("Content-Length", "4")
						w.Header().Set("ETag", etag)
						w.WriteHeader(http.StatusPartialContent)
						n, _ := w.Write(payload[6:])
						sent.Add(int64(n))
						return
					}
					if r.Header.Get("Range") != "" {
						t.Errorf("fallback unexpectedly had Range=%q", r.Header.Get("Range"))
					}
					w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
					n, _ := w.Write(payload)
					sent.Add(int64(n))
				default:
					t.Errorf("unexpected request %d", requests.Load())
				}
			}))
			defer srv.Close()

			dest := filepath.Join(tempDir, "model.bin")
			if err := os.WriteFile(dest, []byte("old destination"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := testDownloadClient(srv.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: srv.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
			if (err != nil) != test.wantErr {
				t.Fatalf("download error=%v, wantErr=%v", err, test.wantErr)
			}
			if requests.Load() != test.wantRequests || sent.Load() != test.wantSent || strings.Join(ranges, ",") != test.wantRanges {
				t.Fatalf("requests=%d body bytes=%d ranges=%q", requests.Load(), sent.Load(), strings.Join(ranges, ","))
			}
			got, readErr := os.ReadFile(dest)
			if test.wantErr {
				if readErr != nil || string(got) != "old destination" {
					t.Fatalf("failed transfer changed destination: %q err=%v", got, readErr)
				}
			} else if readErr != nil || string(got) != string(payload) {
				t.Fatalf("destination=%q err=%v", got, readErr)
			}
			assertNoDownloadTemps(t, dest)
		})
	}
}

func TestSameDownloadURLIncludesUserinfo(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{name: "same-credentials", a: "https://alice:secret@example.test/model?q=1", b: "https://alice:secret@example.test/model?q=1", want: true},
		{name: "different-username", a: "https://alice:secret@example.test/model", b: "https://bob:secret@example.test/model"},
		{name: "different-password", a: "https://alice:secret@example.test/model", b: "https://alice:other@example.test/model"},
		{name: "missing-userinfo", a: "https://alice:secret@example.test/model", b: "https://example.test/model"},
		{name: "empty-password-is-distinct", a: "https://alice@example.test/model", b: "https://alice:@example.test/model"},
		{name: "host-case-insensitive", a: "https://alice:secret@EXAMPLE.test/model", b: "https://alice:secret@example.test/model", want: true},
		{name: "fragment-does-not-change-http-resource", a: "https://alice:secret@example.test/model#one", b: "https://alice:secret@example.test/model#two", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sameDownloadURL(test.a, test.b); got != test.want {
				t.Fatalf("sameDownloadURL(%q,%q)=%v, want %v", test.a, test.b, got, test.want)
			}
		})
	}
}

func TestDownloadFileDoesNotResumeAcrossRedirectedUserinfo(t *testing.T) {
	payload := []byte("credential-scoped-resource")
	const etag = `"same-etag"`
	var oldCredentialsRequests atomic.Int32
	var newCredentialsRequests atomic.Int32
	var sent atomic.Int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "alice" {
			t.Errorf("unexpected authorization header: %q", r.Header.Get("Authorization"))
		}
		if password == "old" {
			if oldCredentialsRequests.Add(1) == 1 {
				location := strings.Replace(server.URL+"/model", "http://", "http://alice:new@", 1)
				http.Redirect(w, r, location, http.StatusFound)
				return
			}
			if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
				t.Errorf("redirected resource identity was reused: headers=%+v", r.Header)
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			n, _ := w.Write(payload)
			sent.Add(int64(n))
			return
		}
		if password != "new" {
			t.Errorf("unexpected password identity %q", password)
		}
		newCredentialsRequests.Add(1)
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack redirected response: %v", err)
			return
		}
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
		_, _ = rw.Write(payload[:5])
		sent.Add(5)
		_ = rw.Flush()
		_ = conn.Close()
	}))
	defer server.Close()

	urlWithOldCredentials := strings.Replace(server.URL+"/model", "http://", "http://alice:old@", 1)
	dest := filepath.Join(t.TempDir(), "model.bin")
	client := testDownloadClient(server.Client())
	err := client.downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: urlWithOldCredentials, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, readErr := os.ReadFile(dest)
	if readErr != nil || string(got) != string(payload) {
		t.Fatalf("destination=%q err=%v", got, readErr)
	}
	if oldCredentialsRequests.Load() != 2 || newCredentialsRequests.Load() != 1 || sent.Load() != int64(5+len(payload)) {
		t.Fatalf("old-source requests=%d redirected requests=%d response bytes=%d", oldCredentialsRequests.Load(), newCredentialsRequests.Load(), sent.Load())
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileResumeHashAttemptTimeoutDoesNotCommit(t *testing.T) {
	payload := []byte("hash-timeout-payload")
	server, requests := newHashValidationServer(t, payload, false)
	defer server.Close()
	client := testDownloadClient(server.Client())
	client.policy.maxAttempts = 2
	hashCalls := atomic.Int32{}
	client.hashFile = func(ctx context.Context, _ string) (string, error) {
		hashCalls.Add(1)
		if _, ok := ctx.Deadline(); !ok {
			return "", errors.New("hash verification context has no attempt deadline")
		}
		<-ctx.Done()
		return "", ctx.Err()
	}
	dest := filepath.Join(t.TempDir(), "model.bin")
	err := client.downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: server.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "attempt timed out while validating") {
		t.Fatalf("download error=%v, want attempt-scoped hash timeout", err)
	}
	if hashCalls.Load() != 1 || requests.Load() != 2 {
		t.Fatalf("hash calls=%d requests=%d", hashCalls.Load(), requests.Load())
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("unverified destination stat=%v", statErr)
	}
	assertNoDownloadTemps(t, dest)
}

func TestDownloadFileResume416HashCancellationDoesNotCommit(t *testing.T) {
	payload := []byte("complete-before-cancel")
	server, requests := newHashValidationServer(t, payload, true)
	defer server.Close()
	client := testDownloadClient(server.Client())
	hashStarted := make(chan struct{})
	client.hashFile = func(ctx context.Context, _ string) (string, error) {
		close(hashStarted)
		<-ctx.Done()
		return "", ctx.Err()
	}
	dest := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(dest, []byte("old destination"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- client.downloadFileWithResume(ctx, File{Path: "model.bin", URL: server.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
	}()
	select {
	case <-hashStarted:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("416 candidate validation did not start")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("download error=%v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("download did not stop after validation cancellation")
	}
	if requests.Load() != 2 {
		t.Fatalf("requests=%d", requests.Load())
	}
	got, readErr := os.ReadFile(dest)
	if readErr != nil || string(got) != "old destination" {
		t.Fatalf("cancellation changed destination: %q err=%v", got, readErr)
	}
	assertNoDownloadTemps(t, dest)
}

func newHashValidationServer(t *testing.T, payload []byte, completeFirstResponse bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	const etag = `"validation-v1"`
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := requests.Add(1)
		if requestNumber == 1 {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack initial response: %v", err)
				return
			}
			prefixLength := len(payload) / 2
			if completeFirstResponse {
				prefixLength = len(payload)
				_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nETag: %s\r\n\r\n%x\r\n", etag, prefixLength)
			} else {
				_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
			}
			_, _ = rw.Write(payload[:prefixLength])
			if completeFirstResponse {
				_, _ = io.WriteString(rw, "\r\n")
			}
			_ = rw.Flush()
			_ = conn.Close()
			return
		}
		if r.Header.Get("Range") == "" || r.Header.Get("If-Range") != etag {
			t.Errorf("resume headers=%+v", r.Header)
		}
		if completeFirstResponse {
			if r.Header.Get("Range") != fmt.Sprintf("bytes=%d-", len(payload)) {
				t.Errorf("complete candidate Range=%q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(payload)))
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start := len(payload) / 2
		if r.Header.Get("Range") != fmt.Sprintf("bytes=%d-", start) {
			t.Errorf("partial candidate Range=%q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(payload)-1, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)-start))
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start:])
	}))
	return server, &requests
}

func TestDownloadFileResumeTemporaryOpenFailuresDoNotCommit(t *testing.T) {
	payload := []byte("open-failure-payload")
	const etag = `"open-v1"`
	tests := []struct {
		name      string
		response  int
		wantSent  int64
		wantError string
	}{
		{name: "append-open", response: http.StatusPartialContent, wantSent: int64(len(payload) - 4), wantError: "open temporary file failed"},
		{name: "truncate-open", response: http.StatusOK, wantSent: int64(len(payload)), wantError: "reset ignored range candidate failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			var requests atomic.Int32
			var sent atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					conn, rw, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("hijack initial response: %v", err)
						return
					}
					_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: %s\r\n\r\n", len(payload), etag)
					_, _ = rw.Write(payload[:4])
					sent.Add(4)
					_ = rw.Flush()
					_ = conn.Close()
					return
				}
				if r.Header.Get("Range") != "bytes=4-" {
					t.Errorf("Range=%q", r.Header.Get("Range"))
				}
				paths, err := filepath.Glob(filepath.Join(tempDir, ".flashhub-download-*"))
				if err != nil || len(paths) != 1 {
					t.Errorf("temporary paths=%v err=%v", paths, err)
					return
				}
				if err := os.Remove(paths[0]); err != nil {
					t.Errorf("remove owned temp to trigger open failure: %v", err)
					return
				}
				if test.response == http.StatusPartialContent {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes 4-%d/%d", len(payload)-1, len(payload)))
					w.Header().Set("Content-Length", fmt.Sprint(len(payload)-4))
					w.Header().Set("ETag", etag)
					w.WriteHeader(http.StatusPartialContent)
					n, _ := w.Write(payload[4:])
					sent.Add(int64(n))
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
				w.Header().Set("ETag", etag)
				n, _ := w.Write(payload)
				sent.Add(int64(n))
			}))
			defer server.Close()

			dest := filepath.Join(tempDir, "model.bin")
			if err := os.WriteFile(dest, []byte("old destination"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := testDownloadClient(server.Client()).downloadFileWithResume(context.Background(), File{Path: "model.bin", URL: server.URL, Size: int64(len(payload)), MD5: payloadMD5(payload)}, dest, true, true, nil, nil, true)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("download error=%v, want %q", err, test.wantError)
			}
			if requests.Load() != 2 || sent.Load() != 4+test.wantSent {
				t.Fatalf("requests=%d body bytes=%d", requests.Load(), sent.Load())
			}
			got, readErr := os.ReadFile(dest)
			if readErr != nil || string(got) != "old destination" {
				t.Fatalf("failed file operation changed destination: %q err=%v", got, readErr)
			}
			assertNoDownloadTemps(t, dest)
		})
	}
}

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestQueue(t *testing.T) *webhookQueue {
	t.Helper()
	q, err := openWebhookQueue(filepath.Join(t.TempDir(), "state", "webhook-queue"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.close() })
	return q
}

func reopenQueue(t *testing.T, q *webhookQueue) *webhookQueue {
	t.Helper()
	q.close()
	r, err := openWebhookQueue(q.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.close() })
	return r
}

// drain removes and returns all queued batches, oldest first.
func drain(t *testing.T, q *webhookQueue) []string {
	t.Helper()
	var bodies []string
	for {
		body, n, ok, err := q.peek()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return bodies
		}
		bodies = append(bodies, string(body))
		if err := q.ack(n); err != nil {
			t.Fatal(err)
		}
	}
}

func testBatch(sha string) []webhookEntry {
	return []webhookEntry{{SHA1: sha, S3: []string{}, File: []string{}}}
}

func testBody(sha string) string {
	return `[{"sha1":"` + sha + `","s3":[],"file":[]}]`
}

func enqueueAll(t *testing.T, q *webhookQueue, shas ...string) {
	t.Helper()
	for _, sha := range shas {
		if err := q.enqueue(testBatch(sha)); err != nil {
			t.Fatal(err)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestWebhookQueueFIFOAndTruncate(t *testing.T) {
	q := newTestQueue(t)
	enqueueAll(t, q, "a", "b", "c")
	if q.pendingBatches() != 3 {
		t.Fatalf("pending = %d", q.pendingBatches())
	}
	if got, want := drain(t, q), []string{testBody("a"), testBody("b"), testBody("c")}; !slices.Equal(got, want) {
		t.Fatalf("drained %q", got)
	}
	if q.pendingBatches() != 0 || fileSize(t, q.path) != queueHeaderSize {
		t.Fatalf("queue not truncated: pending %d, size %d", q.pendingBatches(), fileSize(t, q.path))
	}

	enqueueAll(t, q, "d")
	if got := drain(t, q); !slices.Equal(got, []string{testBody("d")}) {
		t.Fatalf("drained %q after truncate", got)
	}
}

func TestWebhookQueueSurvivesReopen(t *testing.T) {
	q := newTestQueue(t)
	enqueueAll(t, q, "a", "b", "c")
	_, n, _, _ := q.peek()
	if err := q.ack(n); err != nil {
		t.Fatal(err)
	}

	q = reopenQueue(t, q)
	if q.pendingBatches() != 2 {
		t.Fatalf("pending after reopen = %d", q.pendingBatches())
	}
	if got := drain(t, q); !slices.Equal(got, []string{testBody("b"), testBody("c")}) {
		t.Fatalf("drained %q", got)
	}
}

func TestWebhookQueueTrimsTornRecord(t *testing.T) {
	q := newTestQueue(t)
	enqueueAll(t, q, "a")
	size := fileSize(t, q.path)
	q.close()

	f, err := os.OpenFile(q.path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`[{"sha1":"b","s3`)
	f.Close()

	q = reopenQueue(t, q)
	if q.pendingBatches() != 1 || fileSize(t, q.path) != size {
		t.Fatalf("torn record not trimmed: pending %d, size %d, want %d", q.pendingBatches(), fileSize(t, q.path), size)
	}
	enqueueAll(t, q, "c")
	if got := drain(t, q); !slices.Equal(got, []string{testBody("a"), testBody("c")}) {
		t.Fatalf("drained %q", got)
	}
}

func TestWebhookQueueInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook-queue")
	for _, content := range []string{"garbage\n", "S5WQ1 00000000000000000003\n", "S5WQ1 xxxxxxxxxxxxxxxxxxxx\n"} {
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := openWebhookQueue(path); err == nil || !strings.Contains(err.Error(), "delete the file") {
			t.Errorf("header %q: got %v", content, err)
		}
	}
}

func TestWebhookQueueOffsetPastEndIsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook-queue")
	// The state after a crash between truncating and rewriting the header.
	if err := os.WriteFile(path, []byte("S5WQ1 00000000000000000500\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	q, err := openWebhookQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	if q.pendingBatches() != 0 {
		t.Fatalf("pending = %d", q.pendingBatches())
	}
	enqueueAll(t, q, "a")
	if got := drain(t, q); !slices.Equal(got, []string{testBody("a")}) {
		t.Fatalf("drained %q", got)
	}
}

func TestWebhookQueueCompaction(t *testing.T) {
	q := newTestQueue(t)
	q.compactThreshold = 1
	enqueueAll(t, q, "a", "b", "c", "d")
	record := int64(len(testBody("a")) + 1)

	for range 2 {
		_, n, _, _ := q.peek()
		if err := q.ack(n); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := fileSize(t, q.path), queueHeaderSize+2*record; got != want {
		t.Fatalf("size after compaction = %d, want %d", got, want)
	}
	if _, err := os.Stat(q.path + ".compact"); !os.IsNotExist(err) {
		t.Fatal("temporary compaction file left behind")
	}

	enqueueAll(t, q, "e")
	q = reopenQueue(t, q)
	if got := drain(t, q); !slices.Equal(got, []string{testBody("c"), testBody("d"), testBody("e")}) {
		t.Fatalf("drained %q", got)
	}
}

func TestWebhookQueueIsLocked(t *testing.T) {
	q := newTestQueue(t)
	if _, err := openWebhookQueue(q.path); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("got %v", err)
	}
}

func TestWebhookQueuePath(t *testing.T) {
	getenv := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	if got := webhookQueuePath("/custom/q", getenv(map[string]string{"STATE_DIRECTORY": "/var/lib/x"})); got != "/custom/q" {
		t.Errorf("explicit path: got %s", got)
	}
	if got := webhookQueuePath("", getenv(map[string]string{"STATE_DIRECTORY": "/var/lib/s5:/var/lib/other"})); got != "/var/lib/s5/webhook-queue" {
		t.Errorf("STATE_DIRECTORY: got %s", got)
	}
	if got := webhookQueuePath("", getenv(nil)); got != DefaultWebhookQueueFile {
		t.Errorf("default: got %s", got)
	}

	path := filepath.Join(t.TempDir(), "a", "b", "webhook-queue")
	q, err := openWebhookQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	q.close()
	if fileSize(t, path) != queueHeaderSize {
		t.Fatal("queue file not initialised")
	}
}

func TestWebhookQueueMetrics(t *testing.T) {
	conn, addr := listenUDP(t)
	if err := sendMetrics(addr, webhookQueueMetrics(7)); err != nil {
		t.Fatal(err)
	}
	if got := readPackets(t, conn); !slices.Equal(got, []string{"s5commander.webhook.queue_pending:7|g"}) {
		t.Fatalf("payloads = %q", got)
	}
}

type webhookRecorder struct {
	mu       sync.Mutex
	bodies   []string
	headers  []http.Header
	statuses []int
}

func newWebhookServer(t *testing.T, statuses ...int) (*webhookRecorder, *webhookNotifier) {
	t.Helper()
	rec := &webhookRecorder{statuses: statuses}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.bodies = append(rec.bodies, string(body))
		rec.headers = append(rec.headers, r.Header.Clone())
		status := http.StatusOK
		if len(rec.statuses) > 0 {
			status, rec.statuses = rec.statuses[0], rec.statuses[1:]
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	n, err := newWebhookNotifier(srv.URL, "secret-token", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return rec, n
}

func (r *webhookRecorder) take() ([]string, []http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	bodies, headers := r.bodies, r.headers
	r.bodies, r.headers = nil, nil
	return bodies, headers
}

// startSender runs the queue's sender until the test ends or stop is called.
func startSender(t *testing.T, q *webhookQueue, n *webhookNotifier) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.run(ctx, n, time.Millisecond, 4*time.Millisecond)
	}()
	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("sender did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebhookSenderRetriesUntilDelivered(t *testing.T) {
	rec, n := newWebhookServer(t, http.StatusInternalServerError, http.StatusUnauthorized, http.StatusBadRequest)
	q := newTestQueue(t)
	enqueueAll(t, q, "a", "b")

	stop := startSender(t, q, n)
	waitFor(t, func() bool { return q.pendingBatches() == 0 })
	stop()

	bodies, headers := rec.take()
	want := []string{testBody("a"), testBody("a"), testBody("a"), testBody("a"), testBody("b")}
	if !slices.Equal(bodies, want) {
		t.Fatalf("requests = %q, want %q", bodies, want)
	}
	for _, h := range headers {
		if h.Get("Authorization") != "Bearer secret-token" || h.Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected headers %v", h)
		}
	}
	if fileSize(t, q.path) != queueHeaderSize {
		t.Fatal("queue file not truncated after delivery")
	}
}

func TestWebhookSenderDeliversNewBatches(t *testing.T) {
	rec, n := newWebhookServer(t)
	q := newTestQueue(t)
	startSender(t, q, n)

	enqueueAll(t, q, "a")
	waitFor(t, func() bool { return q.pendingBatches() == 0 })
	enqueueAll(t, q, "b")
	waitFor(t, func() bool { return q.pendingBatches() == 0 })

	if bodies, _ := rec.take(); !slices.Equal(bodies, []string{testBody("a"), testBody("b")}) {
		t.Fatalf("requests = %q", bodies)
	}
}

func TestWebhookSenderKeepsBatchesWhileUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	n, err := newWebhookNotifier(srv.URL, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(t)
	enqueueAll(t, q, "a", "b")

	stop := startSender(t, q, n)
	time.Sleep(50 * time.Millisecond)
	stop()

	q = reopenQueue(t, q)
	if got := drain(t, q); !slices.Equal(got, []string{testBody("a"), testBody("b")}) {
		t.Fatalf("batches after outage = %q", got)
	}
}

func TestWebhookWithoutToken(t *testing.T) {
	rec, n := newWebhookServer(t)
	n.token = ""
	if err := n.post(context.Background(), []byte(testBody("x"))); err != nil {
		t.Fatal(err)
	}
	if _, headers := rec.take(); headers[0].Get("Authorization") != "" {
		t.Fatalf("unexpected Authorization header %q", headers[0].Get("Authorization"))
	}
}

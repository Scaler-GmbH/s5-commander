package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DefaultWebhookQueueFile is used when WEBHOOK_QUEUE_FILE is not set and the
// service does not run with a systemd StateDirectory.
const DefaultWebhookQueueFile = "/var/lib/s5-commander/webhook-queue"

// The queue file starts with a fixed-width header holding the offset of the
// oldest undelivered batch, followed by one JSON-encoded batch per line.
const (
	queueMagic      = "S5WQ1 "
	queueOffsetLen  = 20
	queueHeaderSize = int64(len(queueMagic) + queueOffsetLen + 1)
)

// defaultCompactThreshold is how many bytes of delivered batches may
// accumulate at the start of the queue file before it is compacted.
const defaultCompactThreshold = 16 << 20

// webhookQueue is a persistent FIFO of webhook batches. Batches are fsynced on
// enqueue and only removed once delivered, so they survive restarts.
type webhookQueue struct {
	path string

	mu      sync.Mutex
	f       *os.File
	offset  int64
	size    int64
	pending int

	compactThreshold int64
	wake             chan struct{}
}

// webhookQueuePath returns the configured queue file, falling back to the
// systemd state directory and then DefaultWebhookQueueFile.
func webhookQueuePath(configured string, getenv func(string) string) string {
	if configured != "" {
		return configured
	}
	if dir := getenv("STATE_DIRECTORY"); dir != "" {
		// systemd separates multiple state directories with ':'.
		dir, _, _ = strings.Cut(dir, ":")
		return filepath.Join(dir, "webhook-queue")
	}
	return DefaultWebhookQueueFile
}

func openWebhookQueue(path string) (*webhookQueue, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("webhook queue %s: %w (set WEBHOOK_QUEUE_FILE or grant access to the directory)", path, err)
	}
	f, err := openLockedFile(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("webhook queue %s: %w (set WEBHOOK_QUEUE_FILE or grant access to the directory)", path, err)
	}
	q := &webhookQueue{
		path:             path,
		f:                f,
		compactThreshold: defaultCompactThreshold,
		wake:             make(chan struct{}, 1),
	}
	if err := q.load(); err != nil {
		f.Close()
		return nil, err
	}
	return q, nil
}

// openLockedFile opens path and takes an exclusive lock on it, so that two
// processes never share a queue file.
func openLockedFile(path string, flag int) (*os.File, error) {
	f, err := os.OpenFile(path, flag, 0o640)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("file is locked by another process: %w", err)
	}
	return f, nil
}

func (q *webhookQueue) load() error {
	info, err := q.f.Stat()
	if err != nil {
		return fmt.Errorf("webhook queue %s: %w", q.path, err)
	}
	q.size = info.Size()
	if q.size == 0 {
		return q.reset()
	}

	header := make([]byte, queueHeaderSize)
	if q.size < queueHeaderSize {
		return q.corrupt()
	}
	if _, err := q.f.ReadAt(header, 0); err != nil {
		return fmt.Errorf("webhook queue %s: %w", q.path, err)
	}
	digits, ok := bytes.CutPrefix(header, []byte(queueMagic))
	if !ok || header[queueHeaderSize-1] != '\n' {
		return q.corrupt()
	}
	offset, err := strconv.ParseInt(string(digits[:queueOffsetLen]), 10, 64)
	if err != nil || offset < queueHeaderSize {
		return q.corrupt()
	}
	// An offset past the end means everything was delivered and the file was
	// truncated before the header could be rewritten.
	if offset >= q.size {
		return q.reset()
	}
	q.offset = offset

	// Count pending batches and drop a partial last line left by a crash
	// during enqueue.
	end := q.offset
	r := bufio.NewReader(io.NewSectionReader(q.f, q.offset, q.size-q.offset))
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("webhook queue %s: %w", q.path, err)
		}
		end += int64(len(line))
		q.pending++
	}
	if end < q.size {
		log.Printf("Webhook queue %s: discarding %d bytes of an incomplete batch written during a crash", q.path, q.size-end)
		if err := q.f.Truncate(end); err != nil {
			return fmt.Errorf("webhook queue %s: %w", q.path, err)
		}
		q.size = end
		if err := q.f.Sync(); err != nil {
			return fmt.Errorf("webhook queue %s: %w", q.path, err)
		}
	}
	if q.pending == 0 {
		return q.reset()
	}
	return nil
}

func (q *webhookQueue) corrupt() error {
	return fmt.Errorf("webhook queue %s has an invalid header; stop the service and delete the file to clear the queue", q.path)
}

// reset empties the queue file. The header is first pointed at the current
// end, so a crash before the truncate cannot resend delivered batches.
func (q *webhookQueue) reset() error {
	if q.size > queueHeaderSize {
		if err := q.writeHeader(q.size); err != nil {
			return err
		}
	}
	if err := q.f.Truncate(queueHeaderSize); err != nil {
		return fmt.Errorf("webhook queue %s: %w", q.path, err)
	}
	q.offset, q.size, q.pending = queueHeaderSize, queueHeaderSize, 0
	return q.writeHeader(queueHeaderSize)
}

func (q *webhookQueue) writeHeader(offset int64) error {
	return writeQueueHeader(q.f, offset)
}

func writeQueueHeader(f *os.File, offset int64) error {
	header := fmt.Sprintf("%s%0*d\n", queueMagic, queueOffsetLen, offset)
	if _, err := f.WriteAt([]byte(header), 0); err != nil {
		return fmt.Errorf("error writing webhook queue header: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("error syncing webhook queue: %w", err)
	}
	return nil
}

// enqueue appends a batch and syncs it to disk before returning.
func (q *webhookQueue) enqueue(entries []webhookEntry) error {
	body, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("error encoding webhook batch: %w", err)
	}
	line := append(body, '\n')

	q.mu.Lock()
	defer q.mu.Unlock()
	if _, err := q.f.WriteAt(line, q.size); err != nil {
		q.f.Truncate(q.size)
		return fmt.Errorf("error writing webhook queue %s: %w", q.path, err)
	}
	if err := q.f.Sync(); err != nil {
		q.f.Truncate(q.size)
		return fmt.Errorf("error syncing webhook queue %s: %w", q.path, err)
	}
	q.size += int64(len(line))
	q.pending++
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}

// peek returns the oldest undelivered batch and its length in the file.
func (q *webhookQueue) peek() ([]byte, int64, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.offset >= q.size {
		return nil, 0, false, nil
	}
	r := bufio.NewReader(io.NewSectionReader(q.f, q.offset, q.size-q.offset))
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, 0, false, fmt.Errorf("error reading webhook queue %s: %w", q.path, err)
	}
	return line[:len(line)-1], int64(len(line)), true, nil
}

// ack removes the oldest batch, which is n bytes long, after delivery.
func (q *webhookQueue) ack(n int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.offset += n
	q.pending--
	if q.offset >= q.size {
		return q.reset()
	}
	if err := q.writeHeader(q.offset); err != nil {
		return err
	}
	delivered := q.offset - queueHeaderSize
	if delivered >= q.compactThreshold && delivered*2 >= q.size-queueHeaderSize {
		if err := q.compact(); err != nil {
			log.Printf("Error compacting webhook queue %s: %v", q.path, err)
		}
	}
	return nil
}

// compact rewrites the queue file without its delivered batches.
func (q *webhookQueue) compact() error {
	tmpPath := q.path + ".compact"
	tmp, err := openLockedFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	remaining := q.size - q.offset
	if _, err := tmp.Seek(queueHeaderSize, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(tmp, io.NewSectionReader(q.f, q.offset, remaining)); err != nil {
		return err
	}
	if err := writeQueueHeader(tmp, queueHeaderSize); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, q.path); err != nil {
		return err
	}
	ok = true
	if err := syncDir(filepath.Dir(q.path)); err != nil {
		log.Printf("Error syncing directory of webhook queue %s: %v", q.path, err)
	}
	q.f.Close()
	q.f = tmp
	q.offset, q.size = queueHeaderSize, queueHeaderSize+remaining
	return nil
}

func (q *webhookQueue) pendingBatches() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

func webhookQueueMetrics(pending int) []string {
	return []string{fmt.Sprintf("s5commander.webhook.queue_pending:%d|g", pending)}
}

func (q *webhookQueue) close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.f.Close()
}

// run delivers queued batches in order, one per request, until ctx is done.
// Failed deliveries are retried with exponential backoff between minBackoff
// and maxBackoff; batches are never dropped.
func (q *webhookQueue) run(ctx context.Context, n *webhookNotifier, minBackoff, maxBackoff time.Duration) {
	backoff := minBackoff
	failing := false
	for {
		body, size, ok, err := q.peek()
		if err == nil && !ok {
			select {
			case <-ctx.Done():
				return
			case <-q.wake:
			}
			continue
		}
		if err == nil {
			err = n.post(ctx, body)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("Webhook delivery failed (%d batches pending), retrying in %v: %v", q.pendingBatches(), backoff, err)
			failing = true
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		if err := q.ack(size); err != nil {
			log.Printf("Error removing delivered batch from webhook queue: %v", err)
		}
		if failing {
			log.Printf("Webhook delivery recovered, %d batches pending", q.pendingBatches())
			failing = false
		}
		backoff = minBackoff
	}
}

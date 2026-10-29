package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// webhookEntry reports where one source file was copied to in a batch.
type webhookEntry struct {
	SHA1 string   `json:"sha1"`
	S3   []string `json:"s3"`
	File []string `json:"file"`
}

// webhookNotifier POSTs batch results as JSON to a configured endpoint.
type webhookNotifier struct {
	url    string
	token  string
	client *http.Client
}

func newWebhookNotifier(rawURL, token string, timeout time.Duration) (*webhookNotifier, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("webhook-url (or WEBHOOK_URL env var) must be an http or https URL, got %q", rawURL)
	}
	return &webhookNotifier{
		url:    rawURL,
		token:  token,
		client: &http.Client{Timeout: timeout},
	}, nil
}

// post sends one JSON-encoded batch. Any non-2xx response is an error.
func (w *webhookNotifier) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.token != "" {
		req.Header.Set("Authorization", "Bearer "+w.token)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	return nil
}

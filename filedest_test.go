package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func sha1Hex(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permission checks do not apply to root")
	}
}

func TestLoadDestinationsFileDestination(t *testing.T) {
	dir := t.TempDir()
	getenv, environ := envMap(map[string]string{
		"DEST_1_NAME":      "local",
		"DEST_1_FILE_PATH": dir + "/",
	})

	dests, err := loadDestinations(legacyDest("s3://bucket/path/", "/creds"), getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 2 || dests[0].isFile() {
		t.Fatalf("unexpected destinations %+v", dests)
	}
	if d := dests[1]; !d.isFile() || d.Name != "local" || d.FilePath != dir || d.BucketPath != "" {
		t.Fatalf("unexpected file destination %+v", d)
	}
}

func TestLoadDestinationsFileDestinationFirst(t *testing.T) {
	getenv, environ := envMap(map[string]string{
		"DEST_1_FILE_PATH":      t.TempDir(),
		"DEST_2_S3_BUCKET_PATH": "s3://a/",
		"DEST_2_AWS_CREDS_FILE": "/c",
	})

	dests, err := loadDestinations(legacyDest("", ""), getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 2 || !dests[0].isFile() || dests[0].Name != "dest1" || dests[1].BucketPath != "s3://a/" {
		t.Fatalf("unexpected destinations %+v", dests)
	}
	if d, ok := firstS3(dests); !ok || d.Name != "dest2" {
		t.Fatalf("firstS3 = %+v, %v", d, ok)
	}
}

func TestLoadDestinationsFileValidation(t *testing.T) {
	dir := t.TempDir()
	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		legacy  Destination
		env     map[string]string
		wantErr string
	}{
		{
			name:    "both paths",
			legacy:  legacyDest("s3://legacy/", "/c"),
			env:     map[string]string{"DEST_1_FILE_PATH": dir, "DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_CREDS_FILE": "/c"},
			wantErr: "not both",
		},
		{
			name:    "relative path",
			legacy:  legacyDest("s3://legacy/", "/c"),
			env:     map[string]string{"DEST_1_FILE_PATH": "data/out"},
			wantErr: "must be an absolute path",
		},
		{
			name:    "missing directory",
			legacy:  legacyDest("s3://legacy/", "/c"),
			env:     map[string]string{"DEST_1_FILE_PATH": filepath.Join(dir, "missing")},
			wantErr: "no such file or directory",
		},
		{
			name:    "not a directory",
			legacy:  legacyDest("s3://legacy/", "/c"),
			env:     map[string]string{"DEST_1_FILE_PATH": regular},
			wantErr: "is not a directory",
		},
		{
			name:    "no S3 destination",
			legacy:  legacyDest("", ""),
			env:     map[string]string{"DEST_1_FILE_PATH": dir},
			wantErr: "at least one S3 destination",
		},
		{
			name:    "gap after file destination",
			legacy:  legacyDest("s3://legacy/", "/c"),
			env:     map[string]string{"DEST_1_FILE_PATH": dir, "DEST_3_FILE_PATH": dir},
			wantErr: "DEST_3_* variables are set but DEST_2_S3_BUCKET_PATH or DEST_2_FILE_PATH is missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv, environ := envMap(tt.env)
			_, err := loadDestinations(tt.legacy, getenv, environ)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestCopyFileVerified(t *testing.T) {
	src := filepath.Join(t.TempDir(), "in.gz")
	if err := os.WriteFile(src, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	dst := filepath.Join(out, "2026-10-26", "host", "in.gz")

	sum, size, err := copyFileVerified(src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if sum != sha1Hex("hello world") || size != 11 {
		t.Fatalf("got sha1 %s size %d", sum, size)
	}
	if data, err := os.ReadFile(dst); err != nil || string(data) != "hello world" {
		t.Fatalf("destination content %q, %v", data, err)
	}
	entries, _ := os.ReadDir(filepath.Dir(dst))
	if len(entries) != 1 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
}

func TestCopyFileVerifiedFailureLeavesNoFile(t *testing.T) {
	skipIfRoot(t)
	src := filepath.Join(t.TempDir(), "in.gz")
	if err := os.WriteFile(src, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if err := os.Chmod(out, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(out, 0o755) })

	if _, _, err := copyFileVerified(src, filepath.Join(out, "in.gz")); err == nil {
		t.Fatal("expected error copying into read-only directory")
	}
	if entries, _ := os.ReadDir(out); len(entries) != 0 {
		t.Fatalf("unexpected files: %v", entries)
	}
}

// withLocalDest configures the fake s5cmd to record whether each uploaded key
// was already present in dir.
func withLocalDest(t *testing.T, dir string) string {
	t.Helper()
	log := filepath.Join(t.TempDir(), "local-log")
	t.Setenv("FAKE_LOCAL_DIR", dir)
	t.Setenv("FAKE_LOCAL_LOG", log)
	return log
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(path)
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	slices.Sort(lines)
	return lines
}

func fileAndS3Destinations(dir string) []Destination {
	dests := twoDestinations()
	dests[0].BaseURL = "https://primary.s3.amazonaws.com"
	dests[1].BaseURL = "https://backup.example.com"
	return append([]Destination{{Name: "local", FilePath: dir}}, dests...)
}

func TestMultiFileDestinationCopiedBeforeS3(t *testing.T) {
	h := newMultiHarness(t, "a.gz", "sub/sp ace.gz")
	out := t.TempDir()
	localLog := withLocalDest(t, out)
	t.Setenv("FAKE_DST_PREFIX", "s3://primary/logs/")

	p := newMultiProcessor(fileAndS3Destinations(out), h.dataDir, "**/*.gz", h.binary, nil)
	summary, err := p.process()
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"s3://primary/logs/a.gz|1", "s3://primary/logs/sub/sp ace.gz|1"}
	var got []string
	for _, l := range readLines(t, localLog) {
		if strings.HasPrefix(l, "s3://primary/") {
			got = append(got, l)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("local copies not present before S3 upload: %q", got)
	}
	for _, name := range []string{"a.gz", "sub/sp ace.gz"} {
		if data, err := os.ReadFile(filepath.Join(out, name)); err != nil || string(data) != "12345" {
			t.Errorf("local copy of %s: %q, %v", name, data, err)
		}
	}
	for _, f := range h.files {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("%s not deleted after all destinations confirmed", f)
		}
	}
	if summary.FilesDeleted != 2 || summary.PerDest["local"].FilesTransferred != 2 || summary.PerDest["local"].TotalBytes != 10 {
		t.Fatalf("unexpected summary %+v local=%+v", summary, summary.PerDest["local"])
	}
	if len(p.sha1) != 0 {
		t.Fatalf("sha1 cache not cleaned up: %v", p.sha1)
	}
}

func TestMultiFileDestinationFailureKeepsSource(t *testing.T) {
	skipIfRoot(t)
	h := newMultiHarness(t, "a.gz")
	out := t.TempDir()
	if err := os.Chmod(out, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(out, 0o755) })

	p := newMultiProcessor(fileAndS3Destinations(out), h.dataDir, "*.gz", h.binary, nil)
	summary, err := p.process()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(h.files[0]); err != nil {
		t.Fatal("source deleted although the file destination failed")
	}
	if summary.FilesPending != 1 || summary.PerDest["local"].FilesFailed != 1 || summary.PerDest["a"].FilesTransferred != 1 {
		t.Fatalf("unexpected summary %+v local=%+v", summary, summary.PerDest["local"])
	}
	h.attempts(t)

	if err := os.Chmod(out, 0o755); err != nil {
		t.Fatal(err)
	}
	summary, err = p.process()
	if err != nil {
		t.Fatal(err)
	}
	if a := h.attempts(t); a != nil {
		t.Fatalf("re-uploaded to S3 destinations that already confirmed: %q", a)
	}
	if _, err := os.Stat(filepath.Join(out, "a.gz")); err != nil {
		t.Fatal("local copy missing after retry")
	}
	if _, err := os.Stat(h.files[0]); !os.IsNotExist(err) {
		t.Fatal("source not deleted after retry succeeded")
	}
	if summary.FilesDeleted != 1 || summary.PerDest["local"].FilesTransferred != 1 {
		t.Fatalf("unexpected summary %+v", summary)
	}
}

func decodeEntries(t *testing.T, body string) []webhookEntry {
	t.Helper()
	var entries []webhookEntry
	if err := json.Unmarshal([]byte(body), &entries); err != nil {
		t.Fatalf("invalid payload %q: %v", body, err)
	}
	return entries
}

func TestWebhookReportsIncrementalBatches(t *testing.T) {
	h := newMultiHarness(t, "sub/a.gz")
	out := t.TempDir()
	q := newTestQueue(t)
	p := newMultiProcessor(fileAndS3Destinations(out), h.dataDir, "**/*.gz", h.binary, q)

	t.Setenv("FAKE_FAIL_KEY", "KB")
	if _, err := p.process(); err != nil {
		t.Fatal(err)
	}
	bodies := drain(t, q)
	if len(bodies) != 1 {
		t.Fatalf("expected one queued batch, got %q", bodies)
	}
	want := []webhookEntry{{
		SHA1: sha1Hex("12345"),
		S3:   []string{"https://primary.s3.amazonaws.com/logs/sub/a.gz"},
		File: []string{"sub/a.gz"},
	}}
	if got := decodeEntries(t, bodies[0]); len(got) != 1 || got[0].SHA1 != want[0].SHA1 || !slices.Equal(got[0].S3, want[0].S3) || !slices.Equal(got[0].File, want[0].File) {
		t.Fatalf("batch 1 payload = %s", bodies[0])
	}

	t.Setenv("FAKE_FAIL_KEY", "")
	if _, err := p.process(); err != nil {
		t.Fatal(err)
	}
	bodies = drain(t, q)
	if len(bodies) != 1 {
		t.Fatalf("expected one queued batch, got %q", bodies)
	}
	wantBody := `[{"sha1":"` + sha1Hex("12345") + `","s3":["https://backup.example.com/sub/a.gz"],"file":[]}]`
	if bodies[0] != wantBody {
		t.Fatalf("batch 2 payload = %s, want %s", bodies[0], wantBody)
	}
}

func TestWebhookSkipsBatchWithoutSuccess(t *testing.T) {
	h := newMultiHarness(t, "a.gz")
	q := newTestQueue(t)
	dests := []Destination{
		{Name: "a", BucketPath: "s3://primary/logs/", AccessKeyID: "KX", SecretAccessKey: "S", Region: "r", BaseURL: "https://primary.s3.amazonaws.com"},
		{Name: "b", BucketPath: "s3://backup/", AccessKeyID: "KX", SecretAccessKey: "S", Region: "r", BaseURL: "https://s3.us-east-2.amazonaws.com/backup"},
	}
	p := newMultiProcessor(dests, h.dataDir, "*.gz", h.binary, q)

	t.Setenv("FAKE_FAIL_KEY", "KX")
	if _, err := p.process(); err != nil {
		t.Fatal(err)
	}
	if bodies := drain(t, q); len(bodies) != 0 {
		t.Fatalf("unexpected queued batches %q", bodies)
	}

	t.Setenv("FAKE_FAIL_KEY", "")
	if _, err := p.process(); err != nil {
		t.Fatal(err)
	}
	bodies := drain(t, q)
	wantBody := `[{"sha1":"` + sha1Hex("12345") + `","s3":["https://primary.s3.amazonaws.com/logs/a.gz","https://s3.us-east-2.amazonaws.com/backup/a.gz"],"file":[]}]`
	if len(bodies) != 1 || bodies[0] != wantBody {
		t.Fatalf("payload = %q, want %s", bodies, wantBody)
	}
}

func TestMultiKeepsSourcesUntilBatchQueued(t *testing.T) {
	h := newMultiHarness(t, "a.gz")
	q := newTestQueue(t)
	p := newMultiProcessor(fileAndS3Destinations(t.TempDir()), h.dataDir, "*.gz", h.binary, q)

	// A closed file makes every enqueue fail, as a full disk would.
	q.f.Close()
	summary, err := p.process()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(h.files[0]); err != nil {
		t.Fatal("source deleted although its webhook batch was not queued")
	}
	if summary.FilesDeleted != 0 || summary.FilesPending != 1 || len(p.unqueued) != 1 {
		t.Fatalf("unexpected state: summary %+v, unqueued %d", summary, len(p.unqueued))
	}
	h.attempts(t)

	reopened, err := openWebhookQueue(q.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.close() })
	p.queue = reopened
	summary, err = p.process()
	if err != nil {
		t.Fatal(err)
	}
	if a := h.attempts(t); a != nil {
		t.Fatalf("unexpected uploads after delivery: %q", a)
	}
	if _, err := os.Stat(h.files[0]); !os.IsNotExist(err) {
		t.Fatal("source not deleted once its batch was queued")
	}
	if summary.FilesDeleted != 1 || len(p.unqueued) != 0 {
		t.Fatalf("unexpected state: summary %+v, unqueued %d", summary, len(p.unqueued))
	}
	bodies := drain(t, reopened)
	if len(bodies) != 1 || !strings.Contains(bodies[0], `"file":["a.gz"]`) {
		t.Fatalf("queued batches = %q", bodies)
	}
}

func TestDestinationObjectURL(t *testing.T) {
	tests := []struct {
		bucketPath, baseURL, relKey, want string
	}{
		{"s3://my-bucket", "https://my-bucket.s3.amazonaws.com", "auctions/2026-10-28/12/file.gz", "https://my-bucket.s3.amazonaws.com/auctions/2026-10-28/12/file.gz"},
		{"s3://my-bucket/", "https://my-bucket.s3.amazonaws.com", "a.gz", "https://my-bucket.s3.amazonaws.com/a.gz"},
		{"s3://bold-exchange-logs/events/", "https://cdn.example.com", "2026-10-28/h1/x.gz", "https://cdn.example.com/events/2026-10-28/h1/x.gz"},
		{"s3://legacy/path", "https://s3.us-east-2.amazonaws.com/legacy", "a.gz", "https://s3.us-east-2.amazonaws.com/legacy/path/a.gz"},
		{"s3://b/", "https://b.example.com", "sub/sp ace+it's#1?.gz", "https://b.example.com/sub/sp%20ace%2Bit%27s%231%3F.gz"},
	}
	for _, tt := range tests {
		d := Destination{BucketPath: tt.bucketPath, BaseURL: tt.baseURL}
		if got := d.objectURL(tt.relKey); got != tt.want {
			t.Errorf("objectURL(%q) with %q = %q, want %q", tt.relKey, tt.bucketPath, got, tt.want)
		}
	}
}

func TestLoadDestinationsBaseURL(t *testing.T) {
	getenv, environ := envMap(map[string]string{
		"DEST_1_S3_BUCKET_PATH": "s3://a/",
		"DEST_1_AWS_CREDS_FILE": "/c",
		"DEST_1_S3_BASE_URL":    "https://a.s3.amazonaws.com/",
		"DEST_2_FILE_PATH":      t.TempDir(),
	})
	legacy := legacyDest("s3://legacy/", "/c")
	legacy.BaseURL = "https://legacy.s3.amazonaws.com"

	dests, err := loadDestinations(legacy, getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if dests[0].BaseURL != "https://legacy.s3.amazonaws.com" || dests[1].BaseURL != "https://a.s3.amazonaws.com" {
		t.Fatalf("unexpected base URLs %+v", dests)
	}
	if err := requireBaseURLs(dests); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{"s3://a/", "a.s3.amazonaws.com", "https://a.example.com/?x=1"} {
		getenv, environ := envMap(map[string]string{"DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_CREDS_FILE": "/c", "DEST_1_S3_BASE_URL": bad})
		if _, err := loadDestinations(legacyDest("", ""), getenv, environ); err == nil || !strings.Contains(err.Error(), "DEST_1_S3_BASE_URL") {
			t.Errorf("base URL %q: got %v", bad, err)
		}
	}
	bad := legacyDest("s3://legacy/", "/c")
	bad.BaseURL = "legacy.example.com"
	getenv, environ = envMap(nil)
	if _, err := loadDestinations(bad, getenv, environ); err == nil || !strings.Contains(err.Error(), "S3_BASE_URL") {
		t.Errorf("legacy base URL: got %v", err)
	}
}

func TestRequireBaseURLs(t *testing.T) {
	if err := requireBaseURLs([]Destination{legacyDest("s3://a/", "/c")}); err == nil || !strings.Contains(err.Error(), "S3_BASE_URL env var") {
		t.Errorf("legacy: got %v", err)
	}
	getenv, environ := envMap(map[string]string{
		"DEST_1_FILE_PATH":      t.TempDir(),
		"DEST_2_NAME":           "r2",
		"DEST_2_S3_BUCKET_PATH": "s3://a/",
		"DEST_2_AWS_CREDS_FILE": "/c",
	})
	dests, err := loadDestinations(legacyDest("", ""), getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireBaseURLs(dests); err == nil || !strings.Contains(err.Error(), `destination "r2": DEST_2_S3_BASE_URL is required`) {
		t.Errorf("indexed: got %v", err)
	}
}

func TestNewWebhookNotifierRejectsInvalidURL(t *testing.T) {
	for _, u := range []string{"ftp://example.com", "example.com/hook", "http://"} {
		if _, err := newWebhookNotifier(u, "", time.Second); err == nil {
			t.Errorf("expected error for %q", u)
		}
	}
}

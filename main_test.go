package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func envMap(m map[string]string) (func(string) string, []string) {
	var environ []string
	for k, v := range m {
		environ = append(environ, k+"="+v)
	}
	return func(k string) string { return m[k] }, environ
}

func legacyDest(bucket, credsFile string) Destination {
	return Destination{Name: LegacyDestinationName, BucketPath: bucket, CredsFile: credsFile, Profile: "default", Legacy: true}
}

func TestLoadDestinationsLegacyOnly(t *testing.T) {
	getenv, environ := envMap(nil)

	dests, err := loadDestinations(legacyDest("s3://bucket/path", "/creds"), getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 1 || !dests[0].Legacy || dests[0].BucketPath != "s3://bucket/path" {
		t.Fatalf("unexpected destinations %+v", dests)
	}
}

func TestLoadDestinationsLegacyErrorMessagesUnchanged(t *testing.T) {
	getenv, environ := envMap(nil)

	_, err := loadDestinations(legacyDest("", "/creds"), getenv, environ)
	if err == nil || err.Error() != "s3-bucket-path (or S3_BUCKET_PATH env var) is required" {
		t.Fatalf("got %v", err)
	}

	_, err = loadDestinations(legacyDest("s3://bucket/", ""), getenv, environ)
	if err == nil || err.Error() != "Either aws-creds-file (or AWS_CREDS_FILE env var) or AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) are required" {
		t.Fatalf("got %v", err)
	}
}

func TestLoadDestinationsLegacyPlusIndexed(t *testing.T) {
	getenv, environ := envMap(map[string]string{
		"DEST_1_NAME":                  "r2 backup",
		"DEST_1_S3_BUCKET_PATH":        "s3://r2-bucket/logs/",
		"DEST_1_AWS_ENDPOINT_URL":      "https://r2.example.com",
		"DEST_1_AWS_ACCESS_KEY_ID":     "K1",
		"DEST_1_AWS_SECRET_ACCESS_KEY": "S1",
		"DEST_1_AWS_DEFAULT_REGION":    "auto",
		"DEST_2_S3_BUCKET_PATH":        "s3://other",
		"DEST_2_AWS_CREDS_FILE":        "/creds2",
	})

	dests, err := loadDestinations(legacyDest("s3://bucket/path/", "/creds"), getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 3 {
		t.Fatalf("expected 3 destinations, got %+v", dests)
	}
	if dests[0].Name != "default" || !dests[0].Legacy {
		t.Errorf("first destination should be legacy: %+v", dests[0])
	}
	if d := dests[1]; d.Name != "r2_backup" || d.EndpointURL != "https://r2.example.com" || !d.hasEnvCreds() || d.Legacy {
		t.Errorf("unexpected DEST_1: %+v", d)
	}
	if d := dests[2]; d.Name != "dest2" || d.CredsFile != "/creds2" || d.Profile != "default" || d.hasEnvCreds() {
		t.Errorf("unexpected DEST_2: %+v", d)
	}
}

func TestLoadDestinationsIndexedOnly(t *testing.T) {
	getenv, environ := envMap(map[string]string{
		"DEST_1_S3_BUCKET_PATH": "s3://a/",
		"DEST_1_AWS_CREDS_FILE": "/c",
	})

	dests, err := loadDestinations(legacyDest("", ""), getenv, environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 1 || dests[0].Legacy || dests[0].Name != "dest1" {
		t.Fatalf("unexpected destinations %+v", dests)
	}
}

func TestLoadDestinationsValidation(t *testing.T) {
	tests := []struct {
		name    string
		legacy  Destination
		env     map[string]string
		wantErr string
	}{
		{
			name:    "gap in numbering",
			legacy:  legacyDest("", ""),
			env:     map[string]string{"DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_CREDS_FILE": "/c", "DEST_3_S3_BUCKET_PATH": "s3://b/"},
			wantErr: "DEST_3_* variables are set but DEST_2_S3_BUCKET_PATH or DEST_2_FILE_PATH is missing",
		},
		{
			name:    "missing credentials",
			legacy:  legacyDest("", ""),
			env:     map[string]string{"DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_ACCESS_KEY_ID": "K"},
			wantErr: "DEST_1: either DEST_1_AWS_CREDS_FILE",
		},
		{
			name:    "duplicate names",
			legacy:  legacyDest("", ""),
			env:     map[string]string{"DEST_1_NAME": "x", "DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_CREDS_FILE": "/c", "DEST_2_NAME": "x", "DEST_2_S3_BUCKET_PATH": "s3://b/", "DEST_2_AWS_CREDS_FILE": "/c"},
			wantErr: `duplicate destination name "x"`,
		},
		{
			name:    "reserved name",
			legacy:  legacyDest("", ""),
			env:     map[string]string{"DEST_1_NAME": "default", "DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_CREDS_FILE": "/c"},
			wantErr: "is reserved",
		},
		{
			name:    "prefix without trailing slash",
			legacy:  legacyDest("", ""),
			env:     map[string]string{"DEST_1_S3_BUCKET_PATH": "s3://a/prefix", "DEST_1_AWS_CREDS_FILE": "/c"},
			wantErr: "must be a bucket",
		},
		{
			name:    "legacy without credentials alongside indexed",
			legacy:  legacyDest("s3://legacy/", ""),
			env:     map[string]string{"DEST_1_S3_BUCKET_PATH": "s3://a/", "DEST_1_AWS_CREDS_FILE": "/c"},
			wantErr: "Either aws-creds-file",
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

func TestDestinationEnvIsolation(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "LEAK")
	t.Setenv("AWS_ACCESS_KEY_ID", "GLOBAL")
	t.Setenv("AWS_PROFILE", "global-profile")

	d := Destination{Name: "x", AccessKeyID: "K", SecretAccessKey: "S", Region: "R"}
	env := d.env()
	for _, kv := range env {
		if strings.HasPrefix(kv, "AWS_") && !slices.Contains([]string{"AWS_ACCESS_KEY_ID=K", "AWS_SECRET_ACCESS_KEY=S", "AWS_DEFAULT_REGION=R"}, kv) {
			t.Errorf("unexpected inherited variable %q", kv)
		}
	}
	if !slices.Contains(env, "AWS_ACCESS_KEY_ID=K") {
		t.Error("destination credentials missing")
	}

	fileDest := Destination{Name: "y", CredsFile: "/c", Profile: "p"}
	for _, kv := range fileDest.env() {
		if strings.HasPrefix(kv, "AWS_") {
			t.Errorf("unexpected inherited variable %q", kv)
		}
	}

	legacy := Destination{Name: "default", CredsFile: "/c", Profile: "p", Legacy: true}
	if !slices.Contains(legacy.env(), "AWS_SESSION_TOKEN=LEAK") {
		t.Error("legacy destination must keep the inherited environment")
	}
}

func TestDestinationGlobalArgs(t *testing.T) {
	envDest := Destination{EndpointURL: "https://e", AccessKeyID: "K", SecretAccessKey: "S", Region: "R"}
	if got, want := envDest.globalArgs(), []string{"--json", "--log", "info", "--endpoint-url", "https://e"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	fileDest := Destination{CredsFile: "/c", Profile: "p"}
	if got, want := fileDest.globalArgs(), []string{"--json", "--log", "info", "--credentials-file", "/c", "--profile", "p"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// fakeMultiS5cmdScript emulates s5cmd for multi-destination tests:
//   - with --dry-run it prints FAKE_S5CMD_OUTPUT and exits with FAKE_S5CMD_EXIT;
//   - with run it executes each "cp --raw src dst" line of the command file,
//     failing every copy when AWS_ACCESS_KEY_ID equals FAKE_FAIL_KEY.
//
// Every copy attempt is appended to FAKE_S5CMD_LOG as "key|session|src|dst".
// When FAKE_LOCAL_DIR is set, each attempt also appends "dst|present" to
// FAKE_LOCAL_LOG, where present is 1 if FAKE_LOCAL_DIR already holds the file
// under the key relative to FAKE_DST_PREFIX.
const fakeMultiS5cmdScript = `#!/bin/sh
for a in "$@"; do
  if [ "$a" = "--dry-run" ]; then
    if [ -n "$FAKE_S5CMD_OUTPUT" ]; then cat "$FAKE_S5CMD_OUTPUT"; fi
    exit ${FAKE_S5CMD_EXIT:-0}
  fi
done
for a in "$@"; do cmdfile="$a"; done
status=0
while IFS= read -r line; do
  eval "set -- $line"
  src="$3"; dst="$4"
  printf '%s|%s|%s|%s\n' "$AWS_ACCESS_KEY_ID" "$AWS_SESSION_TOKEN" "$src" "$dst" >> "$FAKE_S5CMD_LOG"
  if [ -n "$FAKE_LOCAL_DIR" ]; then
    present=0
    if [ -f "$FAKE_LOCAL_DIR/${dst#"$FAKE_DST_PREFIX"}" ]; then present=1; fi
    printf '%s|%s\n' "$dst" "$present" >> "$FAKE_LOCAL_LOG"
  fi
  if [ -n "$FAKE_FAIL_KEY" ] && [ "$AWS_ACCESS_KEY_ID" = "$FAKE_FAIL_KEY" ]; then
    printf '{"operation":"cp","command":"cp","error":"access denied"}\n'
    status=1
  else
    size=$(wc -c < "$src" | tr -d ' ')
    printf '{"operation":"cp","success":true,"source":"%s","destination":"%s","object":{"type":"file","size":%s}}\n' "$src" "$dst" "$size"
  fi
done < "$cmdfile"
exit $status
`

type multiHarness struct {
	binary  string
	logFile string
	dataDir string
	files   []string
}

func newMultiHarness(t *testing.T, names ...string) *multiHarness {
	t.Helper()
	t.Chdir(t.TempDir())
	dir := t.TempDir()
	h := &multiHarness{
		binary:  filepath.Join(dir, "s5cmd"),
		logFile: filepath.Join(dir, "log"),
		dataDir: t.TempDir(),
	}
	if err := os.WriteFile(h.binary, []byte(fakeMultiS5cmdScript), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_S5CMD_LOG", h.logFile)
	t.Setenv("FAKE_S5CMD_EXIT", "0")
	t.Setenv("FAKE_FAIL_KEY", "")
	t.Setenv("FAKE_LOCAL_DIR", "")
	t.Setenv("AWS_SESSION_TOKEN", "LEAK")

	var discovery []string
	for _, name := range names {
		src := filepath.Join(h.dataDir, name)
		if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(src, []byte("12345"), 0o644); err != nil {
			t.Fatal(err)
		}
		h.files = append(h.files, src)
		discovery = append(discovery, cpResultLine(src, "s3://primary/logs/"+name, 5))
	}
	out := filepath.Join(dir, "discovery")
	if err := os.WriteFile(out, []byte(strings.Join(discovery, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_S5CMD_OUTPUT", out)
	return h
}

func (h *multiHarness) attempts(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(h.logFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(h.logFile)
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	slices.Sort(lines)
	return lines
}

func twoDestinations() []Destination {
	return []Destination{
		{Name: "a", BucketPath: "s3://primary/logs/", AccessKeyID: "KA", SecretAccessKey: "SA", Region: "r"},
		{Name: "b", BucketPath: "s3://backup", AccessKeyID: "KB", SecretAccessKey: "SB", Region: "r"},
	}
}

func TestMultiDeletesOnlyAfterAllDestinationsConfirm(t *testing.T) {
	h := newMultiHarness(t, "a.gz", "sub/sp ace.gz", "it's.gz")
	p := newMultiProcessor(twoDestinations(), h.dataDir, "**/*.gz", h.binary, nil)

	t.Setenv("FAKE_FAIL_KEY", "KB")
	summary, err := p.process()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range h.files {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("%s deleted before all destinations confirmed", f)
		}
	}
	if summary.FilesDeleted != 0 || summary.FilesTransferred != 0 || summary.FilesPending != 3 {
		t.Fatalf("unexpected summary %+v", summary)
	}
	if a, b := summary.PerDest["a"], summary.PerDest["b"]; a.FilesTransferred != 3 || a.TotalBytes != 15 || b.FilesFailed != 3 || b.FilesTransferred != 0 {
		t.Fatalf("unexpected per-destination summary a=%+v b=%+v", a, b)
	}

	attempts := h.attempts(t)
	wantKeys := map[string]string{
		"a.gz":          "s3://backup/a.gz",
		"sub/sp ace.gz": "s3://backup/sub/sp ace.gz",
		"it's.gz":       "s3://backup/it's.gz",
	}
	for name, dst := range wantKeys {
		want := fmt.Sprintf("KB||%s|%s", filepath.Join(h.dataDir, name), dst)
		if !slices.Contains(attempts, want) {
			t.Errorf("missing attempt %q in %q", want, attempts)
		}
		want = fmt.Sprintf("KA||%s|s3://primary/logs/%s", filepath.Join(h.dataDir, name), name)
		if !slices.Contains(attempts, want) {
			t.Errorf("missing attempt %q in %q", want, attempts)
		}
	}

	t.Setenv("FAKE_FAIL_KEY", "")
	summary, err = p.process()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range h.attempts(t) {
		if strings.HasPrefix(a, "KA|") {
			t.Errorf("re-uploaded to destination that already confirmed: %q", a)
		}
	}
	for _, f := range h.files {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Fatalf("%s not deleted after all destinations confirmed", f)
		}
	}
	if summary.FilesDeleted != 3 || summary.FilesTransferred != 3 || summary.TotalBytes != 15 || summary.FilesPending != 0 || summary.PerDest["b"].FilesTransferred != 3 {
		t.Fatalf("unexpected summary %+v", summary)
	}
	if len(p.delivered) != 0 {
		t.Fatalf("delivered map not cleaned up: %v", p.delivered)
	}
	if entries, _ := os.ReadDir("."); len(entries) != 0 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
}

func TestMultiPrunesVanishedFiles(t *testing.T) {
	h := newMultiHarness(t, "a.gz")
	p := newMultiProcessor(twoDestinations(), h.dataDir, "*.gz", h.binary, nil)

	t.Setenv("FAKE_FAIL_KEY", "KB")
	if _, err := p.process(); err != nil {
		t.Fatal(err)
	}
	if len(p.delivered) != 1 {
		t.Fatalf("expected one partially delivered file, got %v", p.delivered)
	}

	t.Setenv("FAKE_S5CMD_OUTPUT", "")
	if _, err := p.process(); err != nil {
		t.Fatal(err)
	}
	if len(p.delivered) != 0 {
		t.Fatalf("expected vanished file to be pruned, got %v", p.delivered)
	}
}

func TestMultiNoMatchIsQuiet(t *testing.T) {
	h := newMultiHarness(t)
	out := filepath.Join(t.TempDir(), "nomatch")
	os.WriteFile(out, []byte(`{"operation":"cp","command":"cp","error":"no match found for \"/data/*.gz\""}`+"\n"), 0o644)
	t.Setenv("FAKE_S5CMD_OUTPUT", out)
	t.Setenv("FAKE_S5CMD_EXIT", "1")

	p := newMultiProcessor(twoDestinations(), h.dataDir, "*.gz", h.binary, nil)
	summary, err := p.process()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if summary.FilesTransferred != 0 || h.attempts(t) != nil {
		t.Fatalf("unexpected activity: %+v", summary)
	}
}

func TestMultiDiscoveryFailureIsReported(t *testing.T) {
	h := newMultiHarness(t)
	out := filepath.Join(t.TempDir(), "err")
	os.WriteFile(out, []byte(`{"operation":"cp","command":"cp","error":"permission denied"}`+"\n"), 0o644)
	t.Setenv("FAKE_S5CMD_OUTPUT", out)
	t.Setenv("FAKE_S5CMD_EXIT", "1")

	p := newMultiProcessor(twoDestinations(), h.dataDir, "*.gz", h.binary, nil)
	if _, err := p.process(); err == nil {
		t.Fatal("expected discovery error")
	}
}

func TestDestinationNetdataPayloads(t *testing.T) {
	conn, addr := listenUDP(t)
	dests := twoDestinations()
	summary := &Summary{
		FilesPending: 2,
		PerDest: map[string]*DestSummary{
			"a": {FilesTransferred: 3, TotalBytes: 1024 * 1024},
			"b": {FilesFailed: 3},
		},
	}

	if err := sendDestinationMetrics(addr, dests, summary, map[string]int64{"a": 1700000000}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"s5commander.current.files_pending:2|g",
		"s5commander.dest.a.current.files_transferred:3|g",
		"s5commander.dest.a.current.files_failed:0|g",
		"s5commander.dest.a.current.megabytes_transferred:1.00|g",
		"s5commander.dest.a.last_success:1700000000|g",
		"s5commander.dest.b.current.files_transferred:0|g",
		"s5commander.dest.b.current.files_failed:3|g",
		"s5commander.dest.b.current.megabytes_transferred:0.00|g",
	}
	if got := readPackets(t, conn); !slices.Equal(got, want) {
		t.Fatalf("payloads = %q", got)
	}

	if err := sendDestinationShutdownMetrics(addr, dests, summary); err != nil {
		t.Fatal(err)
	}
	want = []string{
		"s5commander.dest.a.session.final_files_transferred:3|g",
		"s5commander.dest.a.session.final_files_failed:0|g",
		"s5commander.dest.a.session.final_megabytes_transferred:1.00|g",
		"s5commander.dest.b.session.final_files_transferred:0|g",
		"s5commander.dest.b.session.final_files_failed:3|g",
		"s5commander.dest.b.session.final_megabytes_transferred:0.00|g",
	}
	if got := readPackets(t, conn); !slices.Equal(got, want) {
		t.Fatalf("shutdown payloads = %q", got)
	}
}

func TestShellQuote(t *testing.T) {
	if got := shellQuote("it's a*.gz"); got != `'it'"'"'s a*.gz'` {
		t.Fatalf("got %s", got)
	}
}

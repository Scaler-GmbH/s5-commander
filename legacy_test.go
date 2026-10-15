package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeS5cmdScript records its argv and env, then prints FAKE_S5CMD_OUTPUT (if set)
// and exits with FAKE_S5CMD_EXIT (default 0).
const fakeS5cmdScript = `#!/bin/sh
for a in "$@"; do printf '%s\n' "$a"; done > "$FAKE_S5CMD_ARGS"
env > "$FAKE_S5CMD_ENV"
if [ -n "$FAKE_S5CMD_OUTPUT" ]; then cat "$FAKE_S5CMD_OUTPUT"; fi
exit ${FAKE_S5CMD_EXIT:-0}
`

type fakeS5cmd struct {
	binary   string
	argsFile string
	envFile  string
}

func newFakeS5cmd(t *testing.T) *fakeS5cmd {
	t.Helper()
	dir := t.TempDir()
	f := &fakeS5cmd{
		binary:   filepath.Join(dir, "s5cmd"),
		argsFile: filepath.Join(dir, "args"),
		envFile:  filepath.Join(dir, "env"),
	}
	if err := os.WriteFile(f.binary, []byte(fakeS5cmdScript), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_S5CMD_ARGS", f.argsFile)
	t.Setenv("FAKE_S5CMD_ENV", f.envFile)
	t.Setenv("FAKE_S5CMD_OUTPUT", "")
	t.Setenv("FAKE_S5CMD_EXIT", "0")
	return f
}

func (f *fakeS5cmd) setOutput(t *testing.T, lines ...string) {
	t.Helper()
	out := filepath.Join(filepath.Dir(f.binary), "output")
	if err := os.WriteFile(out, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_S5CMD_OUTPUT", out)
}

func (f *fakeS5cmd) args(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(f.argsFile)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

func (f *fakeS5cmd) env(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(f.envFile)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

func cpResultLine(src, dst string, size int64) string {
	return fmt.Sprintf(`{"operation":"cp","success":true,"source":%q,"destination":%q,"object":{"type":"file","size":%d}}`, src, dst, size)
}

// chdirTemp runs the test in a temp dir so the s5cmd JSON output file lands there.
func chdirTemp(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
}

func listenUDP(t *testing.T) (*net.UDPConn, string) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, conn.LocalAddr().String()
}

func readPackets(t *testing.T, conn *net.UDPConn) []string {
	t.Helper()
	var packets []string
	buf := make([]byte, 2048)
	for {
		conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := conn.Read(buf)
		if err != nil {
			return packets
		}
		packets = append(packets, string(buf[:n]))
	}
}

func TestLegacyRunS5cmdEnvCreds(t *testing.T) {
	chdirTemp(t)
	fake := newFakeS5cmd(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SECRET")
	t.Setenv("AWS_DEFAULT_REGION", "eu-central-1")
	t.Setenv("AWS_SESSION_TOKEN", "TOKEN")

	if err := runS5cmd("/data/", "", "s3://bucket/path/", "", "default", "out.json", "/**/**/*.gz", fake.binary, true); err != nil {
		t.Fatal(err)
	}

	want := []string{"--json", "--log", "info", "cp", "/data/**/**/*.gz", "s3://bucket/path/"}
	if got := fake.args(t); !slices.Equal(got, want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	env := fake.env(t)
	for _, kv := range []string{"AWS_ACCESS_KEY_ID=AKID", "AWS_SECRET_ACCESS_KEY=SECRET", "AWS_DEFAULT_REGION=eu-central-1", "AWS_SESSION_TOKEN=TOKEN"} {
		if !slices.Contains(env, kv) {
			t.Errorf("env missing %q", kv)
		}
	}
}

func TestLegacyRunS5cmdCredsFile(t *testing.T) {
	chdirTemp(t)
	fake := newFakeS5cmd(t)
	t.Setenv("AWS_SESSION_TOKEN", "TOKEN")

	if err := runS5cmd("/data", "https://r2.example.com", "s3://bucket/", "/creds", "r2", "out.json", "*.gz", fake.binary, false); err != nil {
		t.Fatal(err)
	}

	want := []string{"--json", "--log", "info", "--endpoint-url", "https://r2.example.com", "--credentials-file", "/creds", "--profile", "r2", "cp", "/data/*.gz", "s3://bucket/"}
	if got := fake.args(t); !slices.Equal(got, want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	if !slices.Contains(fake.env(t), "AWS_SESSION_TOKEN=TOKEN") {
		t.Error("inherited AWS_SESSION_TOKEN was stripped")
	}
}

func TestLegacyProcessFilesDeletesTransferred(t *testing.T) {
	chdirTemp(t)
	fake := newFakeS5cmd(t)
	data := t.TempDir()
	src := filepath.Join(data, "a.gz")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake.setOutput(t, cpResultLine(src, "s3://bucket/a.gz", 2*1024*1024), `not json`)

	summary, err := processFiles(data, "", "s3://bucket/", "/creds", "default", "*.gz", fake.binary, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesTransferred != 1 || summary.FilesDeleted != 1 || summary.TotalBytes != 2*1024*1024 || len(summary.FilesFailed) != 0 {
		t.Fatalf("unexpected summary %+v", summary)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("source file was not deleted")
	}
	if entries, _ := os.ReadDir("."); len(entries) != 0 {
		t.Fatalf("JSON output file not cleaned up: %v", entries)
	}
}

func TestLegacyProcessFilesNoMatchIsQuiet(t *testing.T) {
	chdirTemp(t)
	fake := newFakeS5cmd(t)
	fake.setOutput(t, `{"error":"no match found for \"/data/*.gz\""}`)
	t.Setenv("FAKE_S5CMD_EXIT", "1")

	summary, err := processFiles("/data", "", "s3://bucket/", "/creds", "default", "*.gz", fake.binary, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if summary.FilesTransferred != 0 {
		t.Fatalf("unexpected summary %+v", summary)
	}
}

func TestLegacyNetdataPayloads(t *testing.T) {
	conn, addr := listenUDP(t)
	summary := &Summary{FilesTransferred: 4, FilesDeleted: 3, TotalBytes: 3 * 1024 * 1024, FilesFailed: []string{"x"}}

	if err := sendToNetdata(addr, summary, 1); err != nil {
		t.Fatal(err)
	}
	got := readPackets(t, conn)
	want := []string{
		"s5commander.current.files_transferred:4|g",
		"s5commander.current.files_deleted:3|g",
		"s5commander.current.megabytes_transferred:3.00|g",
		"s5commander.current.files_failed_delete:1|g",
		"s5commander.current.success_rate:75.00|g",
		"s5commander.runs_completed:1|c",
	}
	if len(got) != len(want)+1 || !slices.Equal(got[:len(want)], want) || !strings.HasPrefix(got[len(want)], "s5commander.last_activity:") {
		t.Fatalf("payloads = %q", got)
	}

	if err := sendShutdownMetrics(addr, summary, 7); err != nil {
		t.Fatal(err)
	}
	got = readPackets(t, conn)
	want = []string{
		"s5commander.session.final_files_transferred:4|g",
		"s5commander.session.final_files_deleted:3|g",
		"s5commander.session.final_megabytes_transferred:3.00|g",
		"s5commander.session.final_files_failed_delete:1|g",
		"s5commander.session.final_success_rate:75.00|g",
		"s5commander.session.total_runs:7|g",
		"s5commander.shutdown:1|c",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("shutdown payloads = %q", got)
	}
}

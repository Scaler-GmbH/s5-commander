package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// DestSummary holds per-destination results of a process run.
type DestSummary struct {
	FilesTransferred int
	FilesFailed      int
	TotalBytes       int64
}

func (s *Summary) add(other Summary) {
	s.FilesTransferred += other.FilesTransferred
	s.FilesDeleted += other.FilesDeleted
	s.TotalBytes += other.TotalBytes
	s.FilesFailed = append(s.FilesFailed, other.FilesFailed...)
	s.FilesPending = other.FilesPending
	for name, ds := range other.PerDest {
		if s.PerDest == nil {
			s.PerDest = map[string]*DestSummary{}
		}
		acc, ok := s.PerDest[name]
		if !ok {
			acc = &DestSummary{}
			s.PerDest[name] = acc
		}
		acc.FilesTransferred += ds.FilesTransferred
		acc.FilesFailed += ds.FilesFailed
		acc.TotalBytes += ds.TotalBytes
	}
}

type discoveredFile struct {
	source string
	relKey string
	size   int64
}

// multiProcessor copies every matching file to all destinations and deletes a
// local file only once every destination has confirmed it.
type multiProcessor struct {
	dests        []Destination
	folderPrefix string
	pathSuffix   string
	s5cmdBinary  string

	// delivered maps a local source path to the destinations that confirmed it.
	delivered   map[string]map[string]bool
	lastSuccess map[string]int64
}

func newMultiProcessor(dests []Destination, folderPrefix, pathSuffix, s5cmdBinary string) *multiProcessor {
	return &multiProcessor{
		dests:        dests,
		folderPrefix: folderPrefix,
		pathSuffix:   pathSuffix,
		s5cmdBinary:  s5cmdBinary,
		delivered:    map[string]map[string]bool{},
		lastSuccess:  map[string]int64{},
	}
}

func (p *multiProcessor) process() (Summary, error) {
	summary := Summary{PerDest: map[string]*DestSummary{}}
	for _, d := range p.dests {
		summary.PerDest[d.Name] = &DestSummary{}
	}

	jobID, err := uuid.NewRandom()
	if err != nil {
		return summary, fmt.Errorf("error generating job ID: %v", err)
	}

	files, err := p.discover(jobID.String())
	if err != nil {
		return summary, fmt.Errorf("error discovering files for job %s: %w", jobID, err)
	}

	current := make(map[string]bool, len(files))
	for _, f := range files {
		current[f.source] = true
	}
	for src := range p.delivered {
		if !current[src] {
			delete(p.delivered, src)
		}
	}
	if len(files) == 0 {
		return summary, nil
	}

	results := make([]destResult, len(p.dests))
	var wg sync.WaitGroup
	for i, d := range p.dests {
		var pending []discoveredFile
		for _, f := range files {
			if !p.delivered[f.source][d.Name] {
				pending = append(pending, f)
			}
		}
		if len(pending) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = p.upload(jobID.String(), d, pending)
		}()
	}
	wg.Wait()

	for i, d := range p.dests {
		r := results[i]
		if r.err != nil {
			log.Printf("Error uploading to destination %q for job %s: %v", d.Name, jobID, r.err)
		}
		ds := summary.PerDest[d.Name]
		ds.FilesFailed = r.attempted - len(r.succeeded)
		for src, size := range r.succeeded {
			if p.delivered[src] == nil {
				p.delivered[src] = map[string]bool{}
			}
			p.delivered[src][d.Name] = true
			ds.FilesTransferred++
			ds.TotalBytes += size
		}
		if len(r.succeeded) > 0 {
			p.lastSuccess[d.Name] = time.Now().Unix()
		}
	}

	for _, f := range files {
		if !p.deliveredEverywhere(f.source) {
			if len(p.delivered[f.source]) > 0 {
				summary.FilesPending++
			}
			continue
		}
		summary.FilesTransferred++
		summary.TotalBytes += f.size
		if err := os.Remove(f.source); err != nil {
			summary.FilesFailed = append(summary.FilesFailed, f.source)
		} else {
			summary.FilesDeleted++
			delete(p.delivered, f.source)
		}
	}

	return summary, nil
}

func (p *multiProcessor) deliveredEverywhere(src string) bool {
	for _, d := range p.dests {
		if !p.delivered[src][d.Name] {
			return false
		}
	}
	return true
}

// discover runs a dry-run copy against the first destination to obtain the
// matching files and the key layout s5cmd would use for them.
func (p *multiProcessor) discover(jobID string) ([]discoveredFile, error) {
	pathSuffix := p.pathSuffix
	if len(pathSuffix) > 0 && pathSuffix[0] == '/' {
		pathSuffix = pathSuffix[1:]
	}
	srcPath := filepath.Join(p.folderPrefix, pathSuffix)
	primary := p.dests[0]

	outFile := fmt.Sprintf("%s-discover.json", jobID)
	defer os.Remove(outFile)

	args := append(primary.globalArgs(), "--dry-run", "cp", srcPath, primary.BucketPath)
	runErr := runS5cmdToFile(p.s5cmdBinary, args, primary.env(), outFile)

	results, err := readJobResults(outFile)
	if err != nil {
		return nil, err
	}

	prefix := primary.keyPrefix()
	var files []discoveredFile
	for _, r := range results {
		if r.Operation != "cp" || !r.Success || r.Object.Type != "file" {
			continue
		}
		relKey, ok := strings.CutPrefix(r.Destination, prefix)
		if !ok {
			log.Printf("Skipping %s: unexpected destination %q from s5cmd", r.Source, r.Destination)
			continue
		}
		if strings.ContainsAny(r.Source, "\n\r") || strings.ContainsAny(relKey, "\n\r") {
			log.Printf("Skipping %s: file names containing newlines are not supported", r.Source)
			continue
		}
		files = append(files, discoveredFile{source: r.Source, relKey: relKey, size: r.Object.Size})
	}

	if runErr != nil && len(files) == 0 {
		if isNoMatch, _ := checkForNoMatchError(outFile); isNoMatch {
			return nil, nil
		}
		return nil, fmt.Errorf("error running s5cmd dry-run: %w", runErr)
	}
	return files, nil
}

type destResult struct {
	attempted int
	succeeded map[string]int64
	err       error
}

// upload copies files to a single destination using an s5cmd command file.
func (p *multiProcessor) upload(jobID string, d Destination, files []discoveredFile) destResult {
	res := destResult{attempted: len(files), succeeded: map[string]int64{}}

	cmdFile := fmt.Sprintf("%s-%s.cmds", jobID, d.Name)
	outFile := fmt.Sprintf("%s-%s.json", jobID, d.Name)
	defer os.Remove(cmdFile)
	defer os.Remove(outFile)

	var b strings.Builder
	prefix := d.keyPrefix()
	for _, f := range files {
		fmt.Fprintf(&b, "cp --raw %s %s\n", shellQuote(f.source), shellQuote(prefix+f.relKey))
	}
	if err := os.WriteFile(cmdFile, []byte(b.String()), 0o600); err != nil {
		res.err = fmt.Errorf("error writing command file: %w", err)
		return res
	}

	args := append(d.globalArgs(), "run", cmdFile)
	runErr := runS5cmdToFile(p.s5cmdBinary, args, d.env(), outFile)

	results, err := readJobResults(outFile)
	if err != nil {
		res.err = err
		return res
	}
	attempted := make(map[string]bool, len(files))
	for _, f := range files {
		attempted[f.source] = true
	}
	for _, r := range results {
		if r.Operation == "cp" && r.Success && r.Object.Type == "file" && attempted[r.Source] {
			res.succeeded[r.Source] = r.Object.Size
		}
	}
	if runErr != nil && len(res.succeeded) < res.attempted {
		res.err = fmt.Errorf("%d of %d uploads failed: %w", res.attempted-len(res.succeeded), res.attempted, runErr)
	}
	return res
}

func runS5cmdToFile(binary string, args, env []string, outFile string) error {
	out, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer out.Close()

	cmd := exec.Command(binary, args...)
	cmd.Env = env
	cmd.Stdout = out
	cmd.Stderr = out
	return cmd.Run()
}

func readJobResults(path string) ([]JobResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening job result file: %w", err)
	}
	defer file.Close()

	var results []JobResult
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var result JobResult
			if json.Unmarshal(line, &result) == nil {
				results = append(results, result)
			}
		}
		if errors.Is(err, io.EOF) {
			return results, nil
		}
		if err != nil {
			return nil, fmt.Errorf("error reading job result file: %w", err)
		}
	}
}

// sendDestinationMetrics sends the metrics that only exist in multi-destination
// mode. The existing s5commander.* metrics are sent separately by sendToNetdata.
func sendDestinationMetrics(address string, dests []Destination, summary *Summary, lastSuccess map[string]int64) error {
	metrics := []string{
		fmt.Sprintf("s5commander.current.files_pending:%d|g", summary.FilesPending),
	}
	for _, d := range dests {
		ds := summary.PerDest[d.Name]
		if ds == nil {
			ds = &DestSummary{}
		}
		prefix := "s5commander.dest." + d.Name
		metrics = append(metrics,
			fmt.Sprintf("%s.current.files_transferred:%d|g", prefix, ds.FilesTransferred),
			fmt.Sprintf("%s.current.files_failed:%d|g", prefix, ds.FilesFailed),
			fmt.Sprintf("%s.current.megabytes_transferred:%.2f|g", prefix, float64(ds.TotalBytes)/(1024*1024)),
		)
		if ts := lastSuccess[d.Name]; ts > 0 {
			metrics = append(metrics, fmt.Sprintf("%s.last_success:%d|g", prefix, ts))
		}
	}
	return sendMetrics(address, metrics)
}

func sendDestinationShutdownMetrics(address string, dests []Destination, summary *Summary) error {
	var metrics []string
	for _, d := range dests {
		ds := summary.PerDest[d.Name]
		if ds == nil {
			ds = &DestSummary{}
		}
		prefix := "s5commander.dest." + d.Name
		metrics = append(metrics,
			fmt.Sprintf("%s.session.final_files_transferred:%d|g", prefix, ds.FilesTransferred),
			fmt.Sprintf("%s.session.final_files_failed:%d|g", prefix, ds.FilesFailed),
			fmt.Sprintf("%s.session.final_megabytes_transferred:%.2f|g", prefix, float64(ds.TotalBytes)/(1024*1024)),
		)
	}
	return sendMetrics(address, metrics)
}

func sendMetrics(address string, metrics []string) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to Netdata at %s: %w", address, err)
	}
	defer conn.Close()

	for _, metric := range metrics {
		fmt.Fprint(conn, metric)
	}
	return nil
}

func logDestinationSummaries(dests []Destination, summary *Summary) {
	for _, d := range dests {
		ds := summary.PerDest[d.Name]
		if ds == nil || (ds.FilesTransferred == 0 && ds.FilesFailed == 0) {
			continue
		}
		log.Printf(
			"  destination %q: %d files transferred, %.2f MB, %d files failed to transfer.",
			d.Name,
			ds.FilesTransferred,
			float64(ds.TotalBytes)/(1024*1024),
			ds.FilesFailed,
		)
	}
	if summary.FilesPending > 0 {
		log.Printf("  %d files awaiting confirmation from all destinations.", summary.FilesPending)
	}
}

// shellQuote quotes s for s5cmd command files, which use POSIX shell quoting.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

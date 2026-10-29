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
	queue        *webhookQueue

	// unqueued holds webhook batches that could not be written to the queue
	// yet. Sources are not deleted while it is non-empty.
	unqueued [][]webhookEntry

	// delivered maps a local source path to the destinations that confirmed it.
	delivered   map[string]map[string]bool
	lastSuccess map[string]int64
	// sha1 caches the SHA-1 of local source files that are still on disk.
	sha1 map[string]string
}

func newMultiProcessor(dests []Destination, folderPrefix, pathSuffix, s5cmdBinary string, queue *webhookQueue) *multiProcessor {
	return &multiProcessor{
		dests:        dests,
		folderPrefix: folderPrefix,
		pathSuffix:   pathSuffix,
		s5cmdBinary:  s5cmdBinary,
		queue:        queue,
		delivered:    map[string]map[string]bool{},
		lastSuccess:  map[string]int64{},
		sha1:         map[string]string{},
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
	for src := range p.sha1 {
		if !current[src] {
			delete(p.sha1, src)
		}
	}
	if len(files) == 0 {
		if p.queue != nil {
			if err := p.flushUnqueued(); err != nil {
				log.Printf("Error queueing webhook batch for job %s: %v", jobID, err)
			}
		}
		return summary, nil
	}

	// Local copies are faster, so every file destination finishes before any
	// S3 upload starts.
	results := make([]destResult, len(p.dests))
	p.runDestinations(jobID.String(), files, results, true)
	p.runDestinations(jobID.String(), files, results, false)

	for i, d := range p.dests {
		r := results[i]
		if r.err != nil {
			log.Printf("Error copying to destination %q for job %s: %v", d.Name, jobID, r.err)
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
		for src, sum := range r.sha1 {
			p.sha1[src] = sum
		}
		if len(r.succeeded) > 0 {
			p.lastSuccess[d.Name] = time.Now().Unix()
		}
	}

	// The batch must be on disk before any source is deleted, so a crash
	// cannot lose the notification for a deleted file.
	queued := true
	if p.queue != nil {
		if entries := p.batchEntries(files, results); len(entries) > 0 {
			p.unqueued = append(p.unqueued, entries)
		}
		if err := p.flushUnqueued(); err != nil {
			log.Printf("Error queueing webhook batch for job %s, keeping source files until it is queued: %v", jobID, err)
			queued = false
		}
	}

	for _, f := range files {
		if !p.deliveredEverywhere(f.source) || !queued {
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
			delete(p.sha1, f.source)
		}
	}

	return summary, nil
}

// flushUnqueued writes batches that could not be queued earlier, oldest first.
func (p *multiProcessor) flushUnqueued() error {
	for len(p.unqueued) > 0 {
		if err := p.queue.enqueue(p.unqueued[0]); err != nil {
			return err
		}
		p.unqueued = p.unqueued[1:]
	}
	return nil
}

// runDestinations copies every file a destination has not yet confirmed, for
// either all file destinations or all S3 destinations, in parallel.
func (p *multiProcessor) runDestinations(jobID string, files []discoveredFile, results []destResult, fileDests bool) {
	var wg sync.WaitGroup
	for i, d := range p.dests {
		if d.isFile() != fileDests {
			continue
		}
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
			if fileDests {
				results[i] = p.copyToDir(d, pending)
			} else {
				results[i] = p.upload(jobID, d, pending)
			}
		}()
	}
	wg.Wait()
}

// batchEntries lists, for every file copied anywhere in this run, the
// locations it was copied to in this run.
func (p *multiProcessor) batchEntries(files []discoveredFile, results []destResult) []webhookEntry {
	var entries []webhookEntry
	for _, f := range files {
		entry := webhookEntry{S3: []string{}, File: []string{}}
		for i, d := range p.dests {
			if _, ok := results[i].succeeded[f.source]; !ok {
				continue
			}
			if d.isFile() {
				entry.File = append(entry.File, f.relKey)
			} else {
				entry.S3 = append(entry.S3, d.objectURL(f.relKey))
			}
		}
		if len(entry.S3) == 0 && len(entry.File) == 0 {
			continue
		}
		sum, ok := p.sha1[f.source]
		if !ok {
			var err error
			if sum, _, err = hashFile(f.source); err != nil {
				log.Printf("Not reporting %s to webhook: %v", f.source, err)
				continue
			}
			p.sha1[f.source] = sum
		}
		entry.SHA1 = sum
		entries = append(entries, entry)
	}
	return entries
}

func (p *multiProcessor) deliveredEverywhere(src string) bool {
	for _, d := range p.dests {
		if !p.delivered[src][d.Name] {
			return false
		}
	}
	return true
}

// discover runs a dry-run copy against the first S3 destination to obtain the
// matching files and the key layout s5cmd would use for them.
func (p *multiProcessor) discover(jobID string) ([]discoveredFile, error) {
	pathSuffix := p.pathSuffix
	if len(pathSuffix) > 0 && pathSuffix[0] == '/' {
		pathSuffix = pathSuffix[1:]
	}
	srcPath := filepath.Join(p.folderPrefix, pathSuffix)
	primary, ok := firstS3(p.dests)
	if !ok {
		return nil, errors.New("no S3 destination configured")
	}

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
	// sha1 holds the verified SHA-1 of sources copied to a file destination.
	sha1 map[string]string
	err  error
}

// copyToDir copies files to a local directory destination, keeping the same
// relative layout as the S3 object keys.
func (p *multiProcessor) copyToDir(d Destination, files []discoveredFile) destResult {
	res := destResult{attempted: len(files), succeeded: map[string]int64{}, sha1: map[string]string{}}
	var firstErr error
	for _, f := range files {
		rel := filepath.FromSlash(f.relKey)
		if !filepath.IsLocal(rel) {
			if firstErr == nil {
				firstErr = fmt.Errorf("refusing to copy %s outside %s: key %q", f.source, d.FilePath, f.relKey)
			}
			continue
		}
		sum, size, err := copyFileVerified(f.source, d.location(f.relKey))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		res.succeeded[f.source] = size
		res.sha1[f.source] = sum
	}
	if firstErr != nil {
		res.err = fmt.Errorf("%d of %d copies failed, first error: %w", res.attempted-len(res.succeeded), res.attempted, firstErr)
	}
	return res
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

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

const (
	// LogLevel defines the logging level for s5cmd
	LogLevel = "info"
	// DateFormat defines the date format for directory paths
	DateFormat = "2006-01-02"
)

// ldflags are set by goreleaser
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// JobResult defines the structure for s5cmd JSON output
type JobResult struct {
	Operation   string `json:"operation"`
	Success     bool   `json:"success"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Object      struct {
		Type string `json:"type"`
		Size int64  `json:"size"`
	} `json:"object"`
}

// Summary holds the summarized results of a process run.
type Summary struct {
	FilesTransferred int
	FilesDeleted     int
	TotalBytes       int64
	FilesFailed      []string
	// FilesPending and PerDest are only populated in multi-destination mode.
	FilesPending int
	PerDest      map[string]*DestSummary
}

// getEnvOrFlag returns the environment variable value if set, otherwise returns the flag value
func getEnvOrFlag(envKey string, flagValue string) string {
	if envValue := os.Getenv(envKey); envValue != "" {
		return envValue
	}
	return flagValue
}

// getEnvOrFlagDuration returns the environment variable value as duration if set, otherwise returns the flag value
func getEnvOrFlagDuration(envKey string, flagValue time.Duration) time.Duration {
	if envValue := os.Getenv(envKey); envValue != "" {
		if duration, err := time.ParseDuration(envValue); err == nil {
			return duration
		}
	}
	return flagValue
}

// getEnvOrFlagBool returns the environment variable value as bool if set, otherwise returns the flag value
func getEnvOrFlagBool(envKey string, flagValue bool) bool {
	if envValue := os.Getenv(envKey); envValue != "" {
		if envValue == "true" || envValue == "1" || envValue == "yes" {
			return true
		}
		if envValue == "false" || envValue == "0" || envValue == "no" {
			return false
		}
	}
	return flagValue
}

func main() {
	// operational flags
	folderPrefix := flag.String("folder-prefix", "/tmp/", "Folder prefix for files to be offloaded (env: FOLDER_PREFIX)")
	pathSuffix := flag.String("path-suffix", "/**/**/*.gz", "the path suffix to use for glob matching (env: PATH_SUFFIX)")
	processInterval := flag.Duration("process-interval", 1*time.Second, "The interval between processing runs (env: PROCESS_INTERVAL)")
	netdataEnabled := flag.Bool("netdata-enabled", false, "Enable sending metrics to Netdata (env: NETDATA_ENABLED)")
	netdataAddress := flag.String("netdata-address", "127.0.0.1:8125", "Netdata statsd address (UDP) (env: NETDATA_ADDRESS)")
	s5cmdBinary := flag.String("s5cmd-binary", "s5cmd", "Full path to s5cmd binary (env: S5CMD_BINARY)")

	// s3-like storage flags
	s3BucketPath := flag.String("s3-bucket-path", "", "S3 bucket path (e.g., s3://my-bucket/path/) (env: S3_BUCKET_PATH)")
	awsCredsFile := flag.String("aws-creds-file", "", "Path to AWS credentials file (env: AWS_CREDS_FILE)")
	awsEndpointURL := flag.String("aws-endpoint-url", "", "Custom AWS endpoint (env: AWS_ENDPOINT_URL)")
	awsProfile := flag.String("aws-profile", "default", "AWS profile to use from credentials file (env: AWS_PROFILE)")

	// webhook flags
	webhookURL := flag.String("webhook-url", "", "HTTP(S) endpoint that receives a JSON summary after each batch (env: WEBHOOK_URL)")
	webhookBearerToken := flag.String("webhook-bearer-token", "", "Bearer token sent in the Authorization header of webhook requests (env: WEBHOOK_BEARER_TOKEN)")
	webhookTimeout := flag.Duration("webhook-timeout", 10*time.Second, "Timeout for each webhook request (env: WEBHOOK_TIMEOUT)")
	s3BaseURL := flag.String("s3-base-url", "", "Public HTTP(S) URL of the S3 bucket, used for webhook object URLs (env: S3_BASE_URL)")
	webhookQueueFile := flag.String("webhook-queue-file", "", "File that holds undelivered webhook batches (default: $STATE_DIRECTORY/webhook-queue or "+DefaultWebhookQueueFile+") (env: WEBHOOK_QUEUE_FILE)")
	webhookMaxBackoff := flag.Duration("webhook-max-backoff", 5*time.Minute, "Maximum delay between webhook delivery retries (env: WEBHOOK_MAX_BACKOFF)")

	flag.Parse()

	// Get actual values from environment variables with flag fallbacks
	actualFolderPrefix := getEnvOrFlag("FOLDER_PREFIX", *folderPrefix)
	actualPathSuffix := getEnvOrFlag("PATH_SUFFIX", *pathSuffix)
	actualProcessInterval := getEnvOrFlagDuration("PROCESS_INTERVAL", *processInterval)
	actualNetdataEnabled := getEnvOrFlagBool("NETDATA_ENABLED", *netdataEnabled)
	actualNetdataAddress := getEnvOrFlag("NETDATA_ADDRESS", *netdataAddress)
	actualS5cmdBinary := getEnvOrFlag("S5CMD_BINARY", *s5cmdBinary)

	// If AWS endpoint, creds file, profile, or S3 bucket path are set via env vars, override flags
	actualAwsEndpointURL := getEnvOrFlag("AWS_ENDPOINT_URL", *awsEndpointURL)
	actualAwsCredsFile := getEnvOrFlag("AWS_CREDS_FILE", *awsCredsFile)
	actualAwsProfile := getEnvOrFlag("AWS_PROFILE", *awsProfile)
	actualS3BucketPath := getEnvOrFlag("S3_BUCKET_PATH", *s3BucketPath)

	actualWebhookURL := getEnvOrFlag("WEBHOOK_URL", *webhookURL)
	actualWebhookBearerToken := getEnvOrFlag("WEBHOOK_BEARER_TOKEN", *webhookBearerToken)
	actualWebhookTimeout := getEnvOrFlagDuration("WEBHOOK_TIMEOUT", *webhookTimeout)
	actualS3BaseURL := getEnvOrFlag("S3_BASE_URL", *s3BaseURL)
	actualWebhookQueueFile := webhookQueuePath(getEnvOrFlag("WEBHOOK_QUEUE_FILE", *webhookQueueFile), os.Getenv)
	actualWebhookMaxBackoff := getEnvOrFlagDuration("WEBHOOK_MAX_BACKOFF", *webhookMaxBackoff)

	// Check for AWS credentials in environment variables
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsDefaultRegion := os.Getenv("AWS_DEFAULT_REGION")
	hasAwsEnvCreds := awsAccessKeyID != "" && awsSecretAccessKey != "" && awsDefaultRegion != ""

	legacyDest := Destination{
		Name:        LegacyDestinationName,
		BucketPath:  actualS3BucketPath,
		EndpointURL: actualAwsEndpointURL,
		CredsFile:   actualAwsCredsFile,
		Profile:     actualAwsProfile,
		BaseURL:     actualS3BaseURL,
		Legacy:      true,
	}
	if hasAwsEnvCreds {
		legacyDest.AccessKeyID = awsAccessKeyID
		legacyDest.SecretAccessKey = awsSecretAccessKey
		legacyDest.Region = awsDefaultRegion
	}

	dests, err := loadDestinations(legacyDest, os.Getenv, os.Environ())
	if err != nil {
		log.Fatal(err)
	}
	var notifier *webhookNotifier
	var queue *webhookQueue
	if actualWebhookURL != "" {
		notifier, err = newWebhookNotifier(actualWebhookURL, actualWebhookBearerToken, actualWebhookTimeout)
		if err != nil {
			log.Fatal(err)
		}
		if err := requireBaseURLs(dests); err != nil {
			log.Fatal(err)
		}
		if actualWebhookMaxBackoff < time.Second {
			log.Fatal("webhook-max-backoff (or WEBHOOK_MAX_BACKOFF env var) must be at least 1s")
		}
		queue, err = openWebhookQueue(actualWebhookQueueFile)
		if err != nil {
			log.Fatal(err)
		}
		defer queue.close()
	}
	// The webhook needs per-file results, which only the multi-destination
	// processor tracks.
	legacyMode := len(dests) == 1 && dests[0].Legacy && notifier == nil

	// Log startup configuration
	if legacyMode {
		if hasAwsEnvCreds {
			log.Printf("Using AWS credentials from environment variables (region: %s)", awsDefaultRegion)
		} else {
			log.Printf("Using AWS credentials from file: %s", actualAwsCredsFile)
		}
	} else {
		for _, d := range dests {
			if d.isFile() {
				log.Printf("Destination %q: %s (local directory)", d.Name, d.FilePath)
			} else if d.hasEnvCreds() {
				log.Printf("Destination %q: %s (credentials from environment variables, region: %s)", d.Name, d.BucketPath, d.Region)
			} else {
				log.Printf("Destination %q: %s (credentials from file: %s, profile: %s)", d.Name, d.BucketPath, d.CredsFile, d.Profile)
			}
		}
	}
	if notifier != nil {
		log.Printf("Sending batch webhooks to %s (bearer token configured: %t)", actualWebhookURL, actualWebhookBearerToken != "")
		log.Printf("Webhook queue: %s (%d batches pending, max retry delay %v)", actualWebhookQueueFile, queue.pendingBatches(), actualWebhookMaxBackoff)
	}
	log.Printf("Using s5cmd binary: %s", actualS5cmdBinary)

	var multi *multiProcessor
	if !legacyMode {
		multi = newMultiProcessor(dests, actualFolderPrefix, actualPathSuffix, actualS5cmdBinary, queue)
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var senderDone chan struct{}
	if queue != nil {
		senderDone = make(chan struct{})
		go func() {
			defer close(senderDone)
			queue.run(ctx, notifier, time.Second, actualWebhookMaxBackoff)
		}()
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start shutdown handler in a goroutine
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	loggingInterval := 1 * time.Minute
	runsPerLog := int(loggingInterval / actualProcessInterval)
	if runsPerLog < 1 {
		runsPerLog = 1
	}

	var accumulatedSummary Summary
	runCounter := 0
	ticker := time.NewTicker(actualProcessInterval)
	defer ticker.Stop()

	log.Printf("s5-commander started, processing every %v", actualProcessInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, finishing current operations...")

			// Send any accumulated metrics before shutdown
			if actualNetdataEnabled && (accumulatedSummary.FilesTransferred > 0 || runCounter > 0) {
				if err := sendShutdownMetrics(actualNetdataAddress, &accumulatedSummary, runCounter); err != nil {
					log.Printf("Error sending final metrics to Netdata: %v", err)
				} else {
					log.Println("Final metrics sent to Netdata")
				}
				if multi != nil {
					if err := sendDestinationShutdownMetrics(actualNetdataAddress, dests, &accumulatedSummary); err != nil {
						log.Printf("Error sending final destination metrics to Netdata: %v", err)
					}
				}
			}

			// Log final summary if there's accumulated data
			if accumulatedSummary.FilesTransferred > 0 || runCounter > 0 {
				totalMegabytes := float64(accumulatedSummary.TotalBytes) / (1024 * 1024)
				log.Printf(
					"Final summary: %d files transferred, %d files deleted, %.2f MB, %d files failed to delete over %d runs.",
					accumulatedSummary.FilesTransferred,
					accumulatedSummary.FilesDeleted,
					totalMegabytes,
					len(accumulatedSummary.FilesFailed),
					runCounter,
				)
				if multi != nil {
					logDestinationSummaries(dests, &accumulatedSummary)
				}
			}

			if senderDone != nil {
				<-senderDone
				log.Printf("Webhook queue closed with %d batches pending", queue.pendingBatches())
			}

			log.Println("s5-commander shutdown complete")
			return

		case <-ticker.C:
			var summary Summary
			var err error
			if multi != nil {
				summary, err = multi.process()
			} else {
				summary, err = processFiles(
					actualFolderPrefix,
					actualAwsEndpointURL,
					actualS3BucketPath,
					actualAwsCredsFile,
					actualAwsProfile,
					actualPathSuffix,
					actualS5cmdBinary,
					hasAwsEnvCreds,
				)
			}
			if err != nil {
				log.Printf("Error processing files: %v", err)
			}

			// Send individual run metrics to Netdata immediately
			if actualNetdataEnabled {
				if err := sendToNetdata(actualNetdataAddress, &summary, 1); err != nil {
					log.Printf("Error sending metrics to Netdata: %v", err)
				}
				if multi != nil {
					if err := sendDestinationMetrics(actualNetdataAddress, dests, &summary, multi.lastSuccess); err != nil {
						log.Printf("Error sending destination metrics to Netdata: %v", err)
					}
				}
				if queue != nil {
					if err := sendMetrics(actualNetdataAddress, webhookQueueMetrics(queue.pendingBatches())); err != nil {
						log.Printf("Error sending webhook queue metrics to Netdata: %v", err)
					}
				}
			}

			accumulatedSummary.add(summary)

			runCounter++

			if runCounter >= runsPerLog {
				if accumulatedSummary.FilesTransferred > 0 {
					totalMegabytes := float64(accumulatedSummary.TotalBytes) / (1024 * 1024)
					log.Printf(
						"Summary over last %d runs (~%v): %d files transferred, %d files deleted, %.2f MB, %d files failed to delete.",
						runCounter,
						loggingInterval,
						accumulatedSummary.FilesTransferred,
						accumulatedSummary.FilesDeleted,
						totalMegabytes,
						len(accumulatedSummary.FilesFailed),
					)
				}
				if multi != nil {
					logDestinationSummaries(dests, &accumulatedSummary)
				}
				runCounter = 0
				accumulatedSummary = Summary{}
			}
		}
	}
}

func processFiles(folderPrefix, awsEndpointURL, s3BucketPath, awsCredsFile, awsProfile, pathSuffix, s5cmdBinary string, hasAwsEnvCreds bool) (Summary, error) {
	jobID, err := uuid.NewRandom()
	if err != nil {
		return Summary{}, fmt.Errorf("error generating job ID: %v", err)
	}

	jsonOutputFile := fmt.Sprintf("%s.json", jobID)
	defer os.Remove(jsonOutputFile)

	err = runS5cmd(folderPrefix, awsEndpointURL, s3BucketPath, awsCredsFile, awsProfile, jsonOutputFile, pathSuffix, s5cmdBinary, hasAwsEnvCreds)
	if err != nil {
		isNoMatchError, _ := checkForNoMatchError(jsonOutputFile)
		if isNoMatchError {
			// Don't log anything here, it's normal to have no files.
			return Summary{}, nil
		}
		return Summary{}, fmt.Errorf("error running s5cmd for job %s: %w", jobID, err)
	}

	summary, err := parseAndCleanup(folderPrefix, jsonOutputFile)
	if err != nil {
		return Summary{}, fmt.Errorf("error parsing results and cleaning up for job %s: %w", jobID, err)
	}

	return summary, nil
}

func runS5cmd(folderPrefix, awsEndpointURL, s3BucketPath, awsCredsFile, awsProfile, jsonOutputFile, pathSuffix, s5cmdBinary string, hasAwsEnvCreds bool) error {
	if len(pathSuffix) > 0 && pathSuffix[0] == '/' {
		pathSuffix = pathSuffix[1:]
	}
	srcPath := filepath.Join(folderPrefix, pathSuffix)
	destPath := s3BucketPath

	var cmd *exec.Cmd

	// build default arguments
	cmdArguments := []string{
		"--json",
		"--log", LogLevel,
	}

	// if we have an endpoint provided, add it to the arguments
	if awsEndpointURL != "" {
		cmdArguments = append(cmdArguments, "--endpoint-url", awsEndpointURL)
	}

	// build the full command based on whether we have env creds or file creds
	if hasAwsEnvCreds {
		cmdArguments = append(cmdArguments, "cp", srcPath, destPath)
		cmd = exec.Command(s5cmdBinary, cmdArguments...)
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", os.Getenv("AWS_ACCESS_KEY_ID")),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", os.Getenv("AWS_SECRET_ACCESS_KEY")),
			fmt.Sprintf("AWS_DEFAULT_REGION=%s", os.Getenv("AWS_DEFAULT_REGION")),
		)
	} else {
		cmdArguments = append(cmdArguments,
			"--credentials-file", awsCredsFile,
			"--profile", awsProfile,
			"cp",
			srcPath,
			destPath,
		)
		cmd = exec.Command(s5cmdBinary, cmdArguments...)
		cmd.Env = os.Environ()
	}

	// redirect output to the JSON output file
	outputFile, err := os.Create(jsonOutputFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer outputFile.Close()

	cmd.Stdout = outputFile
	cmd.Stderr = outputFile

	// log.Printf("Running command: %s", cmd.String())
	return cmd.Run()
}

func sendToNetdata(address string, summary *Summary, runCount int) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to Netdata at %s: %w", address, err)
	}
	defer conn.Close()

	// Calculate derived metrics
	megabytesTransferred := float64(summary.TotalBytes) / (1024 * 1024)
	successRate := 0.0
	if summary.FilesTransferred > 0 {
		successRate = float64(summary.FilesDeleted) / float64(summary.FilesTransferred) * 100.0
	}

	metrics := []string{
		// Current run metrics (these reset each run)
		fmt.Sprintf("s5commander.current.files_transferred:%d|g", summary.FilesTransferred),
		fmt.Sprintf("s5commander.current.files_deleted:%d|g", summary.FilesDeleted),
		fmt.Sprintf("s5commander.current.megabytes_transferred:%.2f|g", megabytesTransferred),
		fmt.Sprintf("s5commander.current.files_failed_delete:%d|g", len(summary.FilesFailed)),
		fmt.Sprintf("s5commander.current.success_rate:%.2f|g", successRate),

		// Operational metrics
		fmt.Sprintf("s5commander.runs_completed:%d|c", runCount),
		fmt.Sprintf("s5commander.last_activity:%d|g", time.Now().Unix()),
	}

	for _, metric := range metrics {
		_, err := fmt.Fprint(conn, metric)
		if err != nil {
			// Log individual metric send errors if needed, but for UDP it's often fire-and-forget
		}
	}
	return nil
}

func checkForNoMatchError(jsonOutputFile string) (bool, error) {
	file, err := os.Open(jsonOutputFile)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("could not open output file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return false, fmt.Errorf("could not read output file: %w", err)
	}

	if len(data) == 0 {
		return false, nil
	}

	var s5Error struct {
		Error string `json:"error"`
	}

	if err := json.Unmarshal(data, &s5Error); err != nil {
		return false, nil
	}

	return strings.Contains(s5Error.Error, "no match found for"), nil
}

func parseAndCleanup(folderPrefix, jsonOutputFile string) (Summary, error) {
	summary := Summary{}

	file, err := os.Open(jsonOutputFile)
	if err != nil {
		return summary, fmt.Errorf("error opening job result file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return summary, fmt.Errorf("error reading job result file: %w", err)
		}

		var result JobResult
		if err := json.Unmarshal(line, &result); err != nil {
			// Ignore unmarshalling errors as some lines may not be valid JSON
			continue
		}

		if result.Operation == "cp" && result.Success && result.Object.Type == "file" {
			summary.FilesTransferred++
			summary.TotalBytes += result.Object.Size

			filePathToDelete := result.Source
			if err := os.Remove(filePathToDelete); err != nil {
				summary.FilesFailed = append(summary.FilesFailed, filePathToDelete)
			} else {
				summary.FilesDeleted++
			}
		}
	}

	return summary, nil
}

func sendShutdownMetrics(address string, summary *Summary, totalRuns int) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to Netdata at %s: %w", address, err)
	}
	defer conn.Close()

	megabytesTransferred := float64(summary.TotalBytes) / (1024 * 1024)
	successRate := 0.0
	if summary.FilesTransferred > 0 {
		successRate = float64(summary.FilesDeleted) / float64(summary.FilesTransferred) * 100.0
	}

	metrics := []string{
		// Final session metrics
		fmt.Sprintf("s5commander.session.final_files_transferred:%d|g", summary.FilesTransferred),
		fmt.Sprintf("s5commander.session.final_files_deleted:%d|g", summary.FilesDeleted),
		fmt.Sprintf("s5commander.session.final_megabytes_transferred:%.2f|g", megabytesTransferred),
		fmt.Sprintf("s5commander.session.final_files_failed_delete:%d|g", len(summary.FilesFailed)),
		fmt.Sprintf("s5commander.session.final_success_rate:%.2f|g", successRate),
		fmt.Sprintf("s5commander.session.total_runs:%d|g", totalRuns),
		fmt.Sprintf("s5commander.shutdown:%d|c", 1),
	}

	for _, metric := range metrics {
		_, err := fmt.Fprint(conn, metric)
		if err != nil {
			// Log individual metric send errors if needed, but for UDP it's often fire-and-forget
		}
	}
	return nil
}

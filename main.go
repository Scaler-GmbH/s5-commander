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
	folderPrefix := flag.String("folder-prefix", "/tmp/", "Folder prefix for files to be offloaded (env: FOLDER_PREFIX)")
	s3BucketPath := flag.String("s3-bucket-path", "", "S3 bucket path (e.g., s3://my-bucket/path/) (env: S3_BUCKET_PATH)")
	awsCredsFile := flag.String("aws-creds-file", "", "Path to AWS credentials file (env: AWS_CREDS_FILE)")
	pathSuffix := flag.String("path-suffix", "/**/**/*.gz", "the path suffix to use for glob matching (env: PATH_SUFFIX)")
	processInterval := flag.Duration("process-interval", 1*time.Second, "The interval between processing runs (env: PROCESS_INTERVAL)")
	netdataEnabled := flag.Bool("netdata-enabled", false, "Enable sending metrics to Netdata (env: NETDATA_ENABLED)")
	netdataAddress := flag.String("netdata-address", "127.0.0.1:8125", "Netdata statsd address (UDP) (env: NETDATA_ADDRESS)")
	s5cmdBinary := flag.String("s5cmd-binary", "s5cmd", "Full path to s5cmd binary (env: S5CMD_BINARY)")
	flag.Parse()

	// Get actual values from environment variables with flag fallbacks
	actualFolderPrefix := getEnvOrFlag("FOLDER_PREFIX", *folderPrefix)
	actualS3BucketPath := getEnvOrFlag("S3_BUCKET_PATH", *s3BucketPath)
	actualAwsCredsFile := getEnvOrFlag("AWS_CREDS_FILE", *awsCredsFile)
	actualPathSuffix := getEnvOrFlag("PATH_SUFFIX", *pathSuffix)
	actualProcessInterval := getEnvOrFlagDuration("PROCESS_INTERVAL", *processInterval)
	actualNetdataEnabled := getEnvOrFlagBool("NETDATA_ENABLED", *netdataEnabled)
	actualNetdataAddress := getEnvOrFlag("NETDATA_ADDRESS", *netdataAddress)
	actualS5cmdBinary := getEnvOrFlag("S5CMD_BINARY", *s5cmdBinary)

	// Check for AWS credentials in environment variables
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsDefaultRegion := os.Getenv("AWS_DEFAULT_REGION")
	hasAwsEnvCreds := awsAccessKeyID != "" && awsSecretAccessKey != "" && awsDefaultRegion != ""

	if actualS3BucketPath == "" {
		log.Fatal("s3-bucket-path (or S3_BUCKET_PATH env var) is required")
	}

	if actualAwsCredsFile == "" && !hasAwsEnvCreds {
		log.Fatal("Either aws-creds-file (or AWS_CREDS_FILE env var) or AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) are required")
	}

	// Log startup configuration
	if hasAwsEnvCreds {
		log.Printf("Using AWS credentials from environment variables (region: %s)", awsDefaultRegion)
	} else {
		log.Printf("Using AWS credentials from file: %s", actualAwsCredsFile)
	}
	log.Printf("Using s5cmd binary: %s", actualS5cmdBinary)

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
				if err := sendToNetdata(actualNetdataAddress, &accumulatedSummary, runCounter); err != nil {
					log.Printf("Error sending final metrics to Netdata: %v", err)
				} else {
					log.Println("Final metrics sent to Netdata")
				}
			}

			// Log final summary if there's accumulated data
			if accumulatedSummary.FilesTransferred > 0 || runCounter > 0 {
				totalMegabytes := float64(accumulatedSummary.TotalBytes) / (1024 * 1024)
				log.Printf(
					"Final summary: %d files transferred, %d files deleted, %.2f MB, %d files failed to delete over %d runs.",
					accumulatedSummary.FilesTransferred, accumulatedSummary.FilesDeleted, totalMegabytes, len(accumulatedSummary.FilesFailed), runCounter,
				)
			}

			log.Println("s5-commander shutdown complete")
			return

		case <-ticker.C:
			summary, err := processFiles(actualFolderPrefix, actualS3BucketPath, actualAwsCredsFile, actualPathSuffix, actualS5cmdBinary, hasAwsEnvCreds)
			if err != nil {
				log.Printf("Error processing files: %v", err)
			}

			accumulatedSummary.FilesTransferred += summary.FilesTransferred
			accumulatedSummary.FilesDeleted += summary.FilesDeleted
			accumulatedSummary.TotalBytes += summary.TotalBytes
			accumulatedSummary.FilesFailed = append(accumulatedSummary.FilesFailed, summary.FilesFailed...)

			runCounter++

			if runCounter >= runsPerLog {
				if accumulatedSummary.FilesTransferred > 0 {
					totalMegabytes := float64(accumulatedSummary.TotalBytes) / (1024 * 1024)
					log.Printf(
						"Summary over last %d runs (~%v): %d files transferred, %d files deleted, %.2f MB, %d files failed to delete.",
						runCounter, loggingInterval, accumulatedSummary.FilesTransferred, accumulatedSummary.FilesDeleted, totalMegabytes, len(accumulatedSummary.FilesFailed),
					)
					if actualNetdataEnabled {
						if err := sendToNetdata(actualNetdataAddress, &accumulatedSummary, runCounter); err != nil {
							log.Printf("Error sending metrics to Netdata: %v", err)
						}
					}
				}
				runCounter = 0
				accumulatedSummary = Summary{}
			}
		}
	}
}

func processFiles(folderPrefix, s3BucketPath, awsCredsFile, pathSuffix, s5cmdBinary string, hasAwsEnvCreds bool) (Summary, error) {
	jobID, err := uuid.NewRandom()
	if err != nil {
		return Summary{}, fmt.Errorf("error generating job ID: %v", err)
	}

	jsonOutputFile := fmt.Sprintf("%s.json", jobID)
	defer os.Remove(jsonOutputFile)

	err = runS5cmd(folderPrefix, s3BucketPath, awsCredsFile, jsonOutputFile, pathSuffix, s5cmdBinary, hasAwsEnvCreds)
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

func runS5cmd(folderPrefix, s3BucketPath, awsCredsFile, jsonOutputFile, pathSuffix, s5cmdBinary string, hasAwsEnvCreds bool) error {
	if len(pathSuffix) > 0 && pathSuffix[0] == '/' {
		pathSuffix = pathSuffix[1:]
	}
	srcPath := filepath.Join(folderPrefix, pathSuffix)
	destPath := s3BucketPath

	var cmd *exec.Cmd
	if hasAwsEnvCreds {
		// Use environment variables for AWS credentials
		cmd = exec.Command(s5cmdBinary,
			"--json",
			"--log", LogLevel,
			"cp",
			srcPath,
			destPath,
		)
		// Ensure AWS environment variables are passed to s5cmd
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", os.Getenv("AWS_ACCESS_KEY_ID")),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", os.Getenv("AWS_SECRET_ACCESS_KEY")),
			fmt.Sprintf("AWS_DEFAULT_REGION=%s", os.Getenv("AWS_DEFAULT_REGION")),
		)
	} else {
		// Use credentials file
		cmd = exec.Command(s5cmdBinary,
			"--json",
			"--log", LogLevel,
			"--credentials-file", awsCredsFile,
			"cp",
			srcPath,
			destPath,
		)
		cmd.Env = os.Environ()
	}

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
		return err
	}
	defer conn.Close()

	metrics := []string{
		fmt.Sprintf("s5commander.files_transferred:%d|g", summary.FilesTransferred),
		fmt.Sprintf("s5commander.files_deleted:%d|g", summary.FilesDeleted),
		fmt.Sprintf("s5commander.megabytes_transferred:%.2f|g", float64(summary.TotalBytes)/(1024*1024)),
		fmt.Sprintf("s5commander.files_failed_delete:%d|g", len(summary.FilesFailed)),
		fmt.Sprintf("s5commander.runs:%d|g", runCount),
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

# S5 Commander

This application is responsible for offloading files from the current server to an S3 bucket and cleaning up the files locally once they have been successfully transferred. It uses the `s5cmd` tool to handle S3 copy operations.

## Prerequisites

- Go 1.22 or higher installed.
- `s5cmd` tool installed and available in the system's PATH.

## Installation

1. Clone the repository or download the source code.
2. Install the dependencies:
   ```sh
   go mod tidy
   ```

## Usage

You can run the application using command line flags or environment variables.

### Using Command Line Flags

```sh
go run main.go \
    --folder-prefix /tmp/ \
    --s3-bucket-path s3://your-s3-bucket/path/ \
    --aws-creds-file /path/to/your/aws/credentials \
    --path-suffix "/**/**/*.gz" \
    --process-interval 30s \
    --netdata-enabled \
    --netdata-address 127.0.0.1:8125 \
    --s5cmd-binary /usr/local/bin/s5cmd
```

### Using Environment Variables

```sh
export FOLDER_PREFIX="/tmp/"
export S3_BUCKET_PATH="s3://your-s3-bucket/path/"
export AWS_CREDS_FILE="/path/to/your/aws/credentials"
export PATH_SUFFIX="/**/**/*.gz"
export PROCESS_INTERVAL="30s"
export NETDATA_ENABLED="true"
export NETDATA_ADDRESS="127.0.0.1:8125"
export S5CMD_BINARY="/usr/local/bin/s5cmd"

go run main.go
```

### Using AWS Environment Variables (Alternative to Credentials File)

Instead of using an AWS credentials file, you can provide credentials via environment variables:

```sh
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
export S3_BUCKET_PATH="s3://your-s3-bucket/path/"
export FOLDER_PREFIX="/tmp/"
export S5CMD_BINARY="/opt/s5cmd/bin/s5cmd"

go run main.go
```

## Configuration Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--folder-prefix` | `FOLDER_PREFIX` | `/tmp/` | Folder prefix for files to be offloaded |
| `--s3-bucket-path` | `S3_BUCKET_PATH` | *(required)* | S3 bucket path (e.g., s3://my-bucket/path/) |
| `--aws-creds-file` | `AWS_CREDS_FILE` | *(see AWS config)* | Path to AWS credentials file |
| `--path-suffix` | `PATH_SUFFIX` | `/**/**/*.gz` | Path suffix to use for glob matching |
| `--process-interval` | `PROCESS_INTERVAL` | `1s` | Interval between processing runs |
| `--netdata-enabled` | `NETDATA_ENABLED` | `false` | Enable sending metrics to Netdata |
| `--netdata-address` | `NETDATA_ADDRESS` | `127.0.0.1:8125` | Netdata statsd address (UDP) |
| `--s5cmd-binary` | `S5CMD_BINARY` | `s5cmd` | Full path to s5cmd binary |

### AWS Credentials Configuration

You have two options for AWS credentials:

1. **Credentials File**: Use `--aws-creds-file` (or `AWS_CREDS_FILE` env var)
2. **Environment Variables**: Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION`

If both are provided, environment variables take precedence. At least one method must be configured.

### S5cmd Binary Configuration

By default, the application expects `s5cmd` to be available in your system's PATH. However, you can specify a custom path to the s5cmd binary using:

- `--s5cmd-binary` flag or `S5CMD_BINARY` environment variable

This is useful when:
- s5cmd is installed in a non-standard location
- You want to use a specific version of s5cmd
- Running in containers where the binary is at a specific path
- You have multiple versions of s5cmd and want to use a particular one

**Examples:**
```sh
# Using a specific installation path
--s5cmd-binary /opt/s5cmd/bin/s5cmd

# Using a version-specific binary
--s5cmd-binary /usr/local/bin/s5cmd-v2.2.2

# In a container environment
export S5CMD_BINARY="/app/bin/s5cmd"
```

## Features

### Graceful Shutdown

The application handles graceful shutdowns when receiving `SIGINT` (Ctrl+C) or `SIGTERM` signals:

- Completes current file processing operations
- Sends final metrics to Netdata (if enabled)
- Logs final summary statistics
- Exits cleanly without data loss

### Netdata Integration

When enabled, the application sends the following metrics to Netdata via StatsD:

- `s5commander.files_transferred`: Number of files successfully transferred
- `s5commander.files_deleted`: Number of files successfully deleted locally
- `s5commander.megabytes_transferred`: Total megabytes transferred
- `s5commander.files_failed_delete`: Number of files that failed to delete locally
- `s5commander.runs`: Number of processing runs completed

### Flexible Configuration

- Command line flags take precedence over environment variables
- Environment variables provide container-friendly configuration
- AWS credentials can be provided via file or environment variables

## How it works

The application runs continuously and performs the following steps in a loop:

1. **Constructs `s5cmd` command**: It builds an `s5cmd` command to copy files matching the specified pattern from the `folder-prefix`.
2. **Executes `s5cmd`**: The command is executed with appropriate AWS credentials, and the JSON output is saved to a temporary file.
3. **Parses the output**: The application parses the JSON output file line by line.
4. **Cleans up files**: For each file that was successfully copied to S3, the corresponding local source file is deleted.
5. **Reports metrics**: Accumulated statistics are logged periodically and optionally sent to Netdata.
6. **Waits**: After each run, the application waits for the specified process interval before starting the next cycle.

The application is designed to be a long-running service that continuously offloads files as they are generated, with proper monitoring and graceful shutdown capabilities. 
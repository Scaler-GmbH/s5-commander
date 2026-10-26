# S5 Commander

This application is responsible for offloading files from the current server to one or more S3 buckets and cleaning up the files locally once they have been successfully transferred to every bucket. It uses the `s5cmd` tool to handle S3 copy operations.

## Prerequisites

- Go 1.27.2 or higher installed.
- `s5cmd` tool installed and available in the system's PATH or path provided via environment variable.

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
go run . \
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

go run .
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

go run .
```

## Configuration Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--folder-prefix` | `FOLDER_PREFIX` | `/tmp/` | Folder prefix for files to be offloaded |
| `--s3-bucket-path` | `S3_BUCKET_PATH` | *(required)* | S3 bucket path (e.g., s3://my-bucket/path/) |
| `--aws-creds-file` | `AWS_CREDS_FILE` | *(see AWS config)* | Path to AWS credentials file |
| `--aws-endpoint-url` | `AWS_ENDPOINT_URL` | `https://s3.amazonaws.com` | Custom AWS endpoint |
| `--aws-profile` | `AWS_PROFILE` | `default` | AWS profile to use from credentials file |
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

Additionally, when using a credentials file, you can specify the AWS profile with `--aws-profile` (or `AWS_PROFILE` env var, default: `default`).

For custom S3-compatible endpoints, use `--aws-endpoint-url` (or `AWS_ENDPOINT_URL` env var, default: `https://s3.amazonaws.com`).

### Multiple Destinations

Files can be copied to several S3-compatible buckets, each with its own endpoint, credentials and metrics. A local file is only deleted once **every** destination has confirmed the upload.

Additional destinations are configured with numbered environment variables, starting at `DEST_1_` and numbered consecutively:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DEST_n_S3_BUCKET_PATH` | *(required)* | Bucket (`s3://bucket`) or prefix ending in `/` (`s3://bucket/path/`) |
| `DEST_n_NAME` | `dest<n>` | Name used in logs and Netdata metrics (characters other than `A-Z a-z 0-9 _ -` become `_`) |
| `DEST_n_AWS_ENDPOINT_URL` | *(none)* | Custom endpoint for this destination |
| `DEST_n_AWS_CREDS_FILE` | *(see below)* | Credentials file for this destination |
| `DEST_n_AWS_PROFILE` | `default` | Profile to use from `DEST_n_AWS_CREDS_FILE` |
| `DEST_n_AWS_ACCESS_KEY_ID` | *(see below)* | Access key for this destination |
| `DEST_n_AWS_SECRET_ACCESS_KEY` | *(see below)* | Secret key for this destination |
| `DEST_n_AWS_DEFAULT_REGION` | *(see below)* | Region for this destination |

Each destination needs either `DEST_n_AWS_CREDS_FILE` or all three of `DEST_n_AWS_ACCESS_KEY_ID`, `DEST_n_AWS_SECRET_ACCESS_KEY` and `DEST_n_AWS_DEFAULT_REGION`. The `s5cmd` process for a numbered destination never inherits other `AWS_*` variables, so credentials cannot leak between destinations.

The existing single-bucket options (`S3_BUCKET_PATH`, `AWS_CREDS_FILE`, `AWS_ACCESS_KEY_ID`, etc.) keep working unchanged and form a destination named `default`. To add a second bucket to an existing deployment, only the `DEST_1_*` variables need to be added. When `DEST_n_*` variables are set, `S3_BUCKET_PATH` becomes optional.

**Example:** keep the existing AWS bucket and add a Cloudflare R2 copy:

```sh
# existing configuration (destination "default")
export S3_BUCKET_PATH="s3://my-aws-bucket/logs/"
export AWS_CREDS_FILE="/etc/s5-commander/aws-credentials"

# additional destination
export DEST_1_NAME="r2"
export DEST_1_S3_BUCKET_PATH="s3://my-r2-bucket/logs/"
export DEST_1_AWS_ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com"
export DEST_1_AWS_ACCESS_KEY_ID="r2-access-key"
export DEST_1_AWS_SECRET_ACCESS_KEY="r2-secret-key"
export DEST_1_AWS_DEFAULT_REGION="auto"

go run .
```

Every destination receives the same object keys (the layout `s5cmd cp` produces for the first destination). If a destination is unavailable, files stay on disk and are retried on the next run; destinations that already confirmed a file are not uploaded to again while the process is running.

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

When enabled, the application sends metrics to Netdata via StatsD after each processing run, providing real-time monitoring:

#### Current Run Metrics (reset each run):
- `s5commander.current.files_transferred`: Files successfully transferred in last run
- `s5commander.current.files_deleted`: Files successfully deleted locally in last run  
- `s5commander.current.megabytes_transferred`: Megabytes transferred in last run
- `s5commander.current.files_failed_delete`: Files that failed to delete in last run
- `s5commander.current.success_rate`: Percentage of transferred files successfully deleted

#### Operational Metrics:
- `s5commander.runs_completed`: Number of processing runs completed
- `s5commander.last_activity`: Unix timestamp of last activity
- `s5commander.shutdown`: Counter incremented on graceful shutdown

#### Session Summary Metrics (sent on shutdown):
- `s5commander.session.final_*`: Final accumulated totals for the session
- `s5commander.session.total_runs`: Total runs completed in the session

#### Multi-Destination Metrics

With a single destination, only the metrics above are sent. When multiple destinations are configured, the metrics above are still sent, where "transferred" means confirmed by all destinations. The following are also sent:

- `s5commander.current.files_pending`: Files confirmed by some, but not all, destinations
- `s5commander.dest.<name>.current.files_transferred`: Files uploaded to this destination in the last run
- `s5commander.dest.<name>.current.files_failed`: Files that failed to upload to this destination in the last run
- `s5commander.dest.<name>.current.megabytes_transferred`: Megabytes uploaded to this destination in the last run
- `s5commander.dest.<name>.last_success`: Unix timestamp of the last successful upload to this destination
- `s5commander.dest.<name>.session.final_*`: Final accumulated per-destination totals (sent on shutdown)

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

With multiple destinations, each run works as follows instead:

1. **Discovers files**: `s5cmd --dry-run cp` against the first destination lists the matching files and their object keys, without uploading anything.
2. **Uploads in parallel**: For each destination, an `s5cmd run` command file copies every file that destination has not yet confirmed, using that destination's own endpoint and credentials.
3. **Deletes only fully delivered files**: A local file is deleted only after every destination has confirmed it. Files with partial delivery stay on disk and are retried on the next run.

The application is designed to be a long-running service that continuously offloads files as they are generated, with proper monitoring and graceful shutdown capabilities. 
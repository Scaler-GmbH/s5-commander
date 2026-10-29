# S5 Commander

This application is responsible for offloading files from the current server to one or more S3 buckets (and optionally local directories) and cleaning up the files locally once they have been successfully transferred to every destination. It uses the `s5cmd` tool to handle S3 copy operations and can notify a webhook after each batch.

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
| `--webhook-url` | `WEBHOOK_URL` | *(none)* | HTTP(S) endpoint that receives a JSON summary after each batch (see [Batch Webhook](#batch-webhook)) |
| `--webhook-bearer-token` | `WEBHOOK_BEARER_TOKEN` | *(none)* | Token sent as `Authorization: Bearer <token>` |
| `--webhook-timeout` | `WEBHOOK_TIMEOUT` | `10s` | Timeout for each webhook request |
| `--s3-base-url` | `S3_BASE_URL` | *(required with webhook)* | Public HTTP(S) URL of the bucket, used for webhook object URLs |

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
| `DEST_n_S3_BASE_URL` | *(required with webhook)* | Public HTTP(S) URL of this bucket, used for webhook object URLs |

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

Every destination receives the same object keys (the layout `s5cmd cp` produces for the first S3 destination). If a destination is unavailable, files stay on disk and are retried on the next run; destinations that already confirmed a file are not uploaded to again while the process is running.

### Local Directory Destinations

A destination can also be a local directory (for example a mounted volume) instead of an S3 bucket. Set `DEST_n_FILE_PATH` instead of `DEST_n_S3_BUCKET_PATH`:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DEST_n_FILE_PATH` | *(required)* | Absolute path to an existing directory. Cannot be combined with `DEST_n_S3_BUCKET_PATH` |
| `DEST_n_NAME` | `dest<n>` | Name used in logs and Netdata metrics |

No credentials are needed for a local directory. At least one S3 destination is still required, because the key layout is taken from the S3 dry-run.

Files are written under the same prefixes as the S3 object keys. If the S3 key is `s3://my-aws-bucket/logs/2026-10-26/host1/file.jsonl.gz` and `DEST_n_FILE_PATH` is `/mnt/data`, the local copy is `/mnt/data/2026-10-26/host1/file.jsonl.gz`. Missing directories are created.

Local copies are made **before** any S3 upload starts. Each copy is written to a temporary file in the target directory, synced to disk, re-read and compared to the source by SHA-1 and size, and only then renamed into place. A failed or mismatched copy is discarded and retried on the next run, and the source file is kept until every destination (local and S3) has confirmed it.

**Example:** an S3 bucket plus a local copy:

```sh
export S3_BUCKET_PATH="s3://my-aws-bucket/logs/"
export AWS_CREDS_FILE="/etc/s5-commander/aws-credentials"

export DEST_1_NAME="nas"
export DEST_1_FILE_PATH="/mnt/data"

go run .
```

Destinations can be listed in any order, so a local directory can just as well be `DEST_1` with the S3 bucket as `DEST_2`.

### Batch Webhook

When `WEBHOOK_URL` is set, a JSON array is POSTed to it after each processing run (batch) in which at least one file was copied somewhere. If `WEBHOOK_BEARER_TOKEN` is set, it is sent as `Authorization: Bearer <token>`. Prefer the environment variable over the flag, because command line flags are visible in the process list.

Each S3 destination needs a base URL when the webhook is enabled, otherwise the application refuses to start. This is the HTTP(S) URL of the bucket itself, for example `https://my-aws-bucket.s3.amazonaws.com`, a path-style `https://s3.us-east-2.amazonaws.com/my-aws-bucket`, or a public R2 or CDN domain:

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--s3-base-url` | `S3_BASE_URL` | Base URL of the `S3_BUCKET_PATH` bucket |
| | `DEST_n_S3_BASE_URL` | Base URL of the `DEST_n_S3_BUCKET_PATH` bucket |

Each entry describes one source file, identified by its SHA-1, and lists the locations it was successfully copied to **in that batch**:

- `s3` holds the object URLs: the destination's base URL followed by the full object key, including any prefix from the bucket path. Characters that are not URL-safe are percent-encoded.
- `file` holds paths relative to `DEST_n_FILE_PATH`, without a leading slash. These are the same as the object keys below the bucket path.

With `S3_BUCKET_PATH=s3://my-aws-bucket/logs/`, `S3_BASE_URL=https://my-aws-bucket.s3.amazonaws.com`, an R2 destination with `DEST_1_S3_BASE_URL=https://pub-1234.r2.dev` and `DEST_2_FILE_PATH=/mnt/data`, a file stored as `/mnt/data/2026-10-26/host1/file.jsonl.gz` is reported as:

```json
[
  {
    "sha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
    "s3": ["https://my-aws-bucket.s3.amazonaws.com/logs/2026-10-26/host1/file.jsonl.gz", "https://pub-1234.r2.dev/logs/2026-10-26/host1/file.jsonl.gz"],
    "file": ["2026-10-26/host1/file.jsonl.gz"]
  }
]
```

- A destination that failed for a file is left out of that file's `s3` or `file` list. `s3` and `file` are always arrays, possibly empty.
- When a failed destination succeeds on a later run, that later batch reports the file again with only the newly copied location. Every successful copy appears in exactly one batch.
- Files that were not copied anywhere in a batch are not included, and no batch is produced for a run without any successful copy.

Setting `WEBHOOK_URL` with only the single `S3_BUCKET_PATH` destination switches to the multi-destination processing described below, because per-file results are needed.

#### Delivery and the Retry Queue

Batches are never dropped. Each batch is written to a queue file on disk, and synced, before any source file of that run is deleted. A background sender then delivers the queue:

- One batch per POST, oldest first, exactly as it was produced. A batch that cannot be delivered holds back the ones behind it, so the order is preserved.
- Any 2xx response counts as delivered. Anything else (connection errors, timeouts, 4xx and 5xx responses) is retried with exponential backoff: 1s, 2s, 4s and so on, up to `WEBHOOK_MAX_BACKOFF`. Retries continue until the batch is delivered, including after restarts.
- Delivery is at-least-once: if the service stops between a successful POST and recording it, that one batch is sent again after the restart. Receivers can de-duplicate using `sha1` plus the location.
- Webhook delivery runs independently of file processing, so a slow or unavailable endpoint does not delay offloading.
- If the queue file cannot be written (for example, the disk is full), the batch is kept in memory and source files are not deleted until it has been queued.

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--webhook-queue-file` | `WEBHOOK_QUEUE_FILE` | see below | File holding undelivered batches |
| `--webhook-max-backoff` | `WEBHOOK_MAX_BACKOFF` | `5m` | Longest delay between delivery attempts (at least `1s`) |

Without `WEBHOOK_QUEUE_FILE`, the queue is stored at `$STATE_DIRECTORY/webhook-queue` when running under systemd with `StateDirectory=` set, and at `/var/lib/s5-commander/webhook-queue` otherwise. Missing directories are created. If the location is not writable, the service refuses to start; set `WEBHOOK_QUEUE_FILE` or grant access. With systemd, a state directory owned by the service user is the simplest option:

```ini
[Service]
StateDirectory=s5-commander
```

The file is locked while the service runs, so two instances cannot share it. It is not meant to be edited: it starts with a header recording delivery progress, followed by one batch per line. It shrinks back to just the header whenever the queue is empty, and is compacted during long outages.

**Clearing the backlog:** if queued batches should be discarded (for example, the endpoint has moved and will never accept them), stop the service, delete the queue file, and start the service again. This is the only way batches are ever removed without being delivered.

The number of queued batches is logged at startup and shutdown, and sent to Netdata as `s5commander.webhook.queue_pending` after each run.

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

When the webhook is enabled, `s5commander.webhook.queue_pending` reports the number of batches waiting to be delivered.

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

With multiple destinations (or a webhook), each run works as follows instead:

1. **Discovers files**: `s5cmd --dry-run cp` against the first S3 destination lists the matching files and their object keys, without uploading anything.
2. **Copies to local directories first**: For each local directory destination, every file that destination has not yet confirmed is copied under the same key, verified by SHA-1 and renamed into place.
3. **Uploads in parallel**: For each S3 destination, an `s5cmd run` command file copies every file that destination has not yet confirmed, using that destination's own endpoint and credentials.
4. **Queues the webhook batch**: If configured, the locations each file was copied to in this run are written to the on-disk webhook queue. A background sender POSTs queued batches to the webhook, retrying until each one is delivered.
5. **Deletes only fully delivered files**: A local file is deleted only after every destination has confirmed it (and, with a webhook, after its batch is queued). Files with partial delivery stay on disk and are retried on the next run.

The application is designed to be a long-running service that continuously offloads files as they are generated, with proper monitoring and graceful shutdown capabilities. 
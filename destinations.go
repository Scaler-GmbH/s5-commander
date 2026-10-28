package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// LegacyDestinationName is the name of the destination built from the original
// single-bucket flags and environment variables.
const LegacyDestinationName = "default"

const (
	legacyBucketRequiredMsg = "s3-bucket-path (or S3_BUCKET_PATH env var) is required"
	legacyCredsRequiredMsg  = "Either aws-creds-file (or AWS_CREDS_FILE env var) or AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) are required"
)

var (
	destVarPattern    = regexp.MustCompile(`^DEST_([0-9]+)_`)
	invalidNameChars  = regexp.MustCompile(`[^A-Za-z0-9_-]`)
	bareBucketPattern = regexp.MustCompile(`^s3://[^/]+$`)
)

// Destination is one target that every file is copied to: either an
// S3-compatible bucket (BucketPath) or a local directory (FilePath).
type Destination struct {
	Name            string
	BucketPath      string
	FilePath        string
	EndpointURL     string
	CredsFile       string
	Profile         string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	// BaseURL is the public HTTP(S) URL of the bucket, used to report object
	// locations to the webhook.
	BaseURL string
	// index is n for destinations configured via DEST_n_* variables.
	index int
	// Legacy marks the destination configured via the original single-bucket
	// options. Its s5cmd process inherits the full environment, as before.
	Legacy bool
}

func (d Destination) hasEnvCreds() bool {
	return d.AccessKeyID != "" && d.SecretAccessKey != "" && d.Region != ""
}

func (d Destination) isFile() bool {
	return d.FilePath != ""
}

// location returns where a file with the given relative key is stored at this
// destination.
func (d Destination) location(relKey string) string {
	if d.isFile() {
		return filepath.Join(d.FilePath, filepath.FromSlash(relKey))
	}
	return d.keyPrefix() + relKey
}

// objectURL returns the HTTP(S) URL of the object stored under relKey: the
// base URL followed by the object key within the bucket.
func (d Destination) objectURL(relKey string) string {
	_, prefix, _ := strings.Cut(strings.TrimPrefix(d.keyPrefix(), "s3://"), "/")
	segments := strings.Split(prefix+relKey, "/")
	for i, s := range segments {
		// S3 decodes '+' in paths as a space, so it must be escaped too.
		segments[i] = strings.ReplaceAll(url.PathEscape(s), "+", "%2B")
	}
	return d.BaseURL + "/" + strings.Join(segments, "/")
}

// validateBaseURL checks a configured base URL and returns it without a
// trailing slash.
func validateBaseURL(envVar, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%s: %q must be an http or https URL without query or fragment", envVar, raw)
	}
	return strings.TrimRight(raw, "/"), nil
}

// requireBaseURLs ensures every S3 destination can be reported to the webhook.
func requireBaseURLs(dests []Destination) error {
	for _, d := range dests {
		if d.isFile() || d.BaseURL != "" {
			continue
		}
		if d.Legacy {
			return fmt.Errorf("s3-base-url (or S3_BASE_URL env var) is required when the webhook is enabled")
		}
		return fmt.Errorf("destination %q: DEST_%d_S3_BASE_URL is required when the webhook is enabled", d.Name, d.index)
	}
	return nil
}

// keyPrefix returns the bucket path in the form s5cmd uses as the prefix of
// destination keys for wildcard copies.
func (d Destination) keyPrefix() string {
	if strings.HasSuffix(d.BucketPath, "/") {
		return d.BucketPath
	}
	return d.BucketPath + "/"
}

// globalArgs returns the s5cmd arguments that precede the subcommand.
func (d Destination) globalArgs() []string {
	args := []string{"--json", "--log", LogLevel}
	if d.EndpointURL != "" {
		args = append(args, "--endpoint-url", d.EndpointURL)
	}
	if !d.hasEnvCreds() {
		args = append(args, "--credentials-file", d.CredsFile, "--profile", d.Profile)
	}
	return args
}

// env returns the environment for this destination's s5cmd process. Non-legacy
// destinations never see AWS_* variables other than their own.
func (d Destination) env() []string {
	var env []string
	if d.Legacy {
		env = os.Environ()
	} else {
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "AWS_") {
				env = append(env, kv)
			}
		}
	}
	if d.hasEnvCreds() {
		env = append(env,
			"AWS_ACCESS_KEY_ID="+d.AccessKeyID,
			"AWS_SECRET_ACCESS_KEY="+d.SecretAccessKey,
			"AWS_DEFAULT_REGION="+d.Region,
		)
	}
	return env
}

// loadDestinations combines the legacy destination with any DEST_n_* destinations
// found in the environment. Without DEST_n_* variables the legacy destination is
// validated exactly as before and is the only destination.
func loadDestinations(legacy Destination, getenv func(string) string, environ []string) ([]Destination, error) {
	indexed, err := loadIndexedDestinations(getenv, environ)
	if err != nil {
		return nil, err
	}
	if legacy.BaseURL != "" {
		if legacy.BaseURL, err = validateBaseURL("S3_BASE_URL", legacy.BaseURL); err != nil {
			return nil, err
		}
	}

	if len(indexed) == 0 {
		if legacy.BucketPath == "" {
			return nil, fmt.Errorf("%s", legacyBucketRequiredMsg)
		}
		if legacy.CredsFile == "" && !legacy.hasEnvCreds() {
			return nil, fmt.Errorf("%s", legacyCredsRequiredMsg)
		}
		return []Destination{legacy}, nil
	}

	var dests []Destination
	if legacy.BucketPath != "" {
		if legacy.CredsFile == "" && !legacy.hasEnvCreds() {
			return nil, fmt.Errorf("%s", legacyCredsRequiredMsg)
		}
		dests = append(dests, legacy)
	}
	dests = append(dests, indexed...)

	seen := map[string]bool{}
	hasS3 := false
	for _, d := range dests {
		if seen[d.Name] {
			return nil, fmt.Errorf("duplicate destination name %q", d.Name)
		}
		seen[d.Name] = true
		if d.isFile() {
			continue
		}
		hasS3 = true
		if !strings.HasPrefix(d.BucketPath, "s3://") || (!strings.HasSuffix(d.BucketPath, "/") && !bareBucketPattern.MatchString(d.BucketPath)) {
			return nil, fmt.Errorf("destination %q: bucket path %q must be a bucket (s3://bucket) or a prefix ending in '/'", d.Name, d.BucketPath)
		}
	}
	// The object key layout is taken from an s5cmd dry-run against an S3
	// destination, so file destinations cannot be used on their own.
	if !hasS3 {
		return nil, fmt.Errorf("at least one S3 destination (S3_BUCKET_PATH or DEST_n_S3_BUCKET_PATH) is required")
	}
	return dests, nil
}

func firstS3(dests []Destination) (Destination, bool) {
	for _, d := range dests {
		if !d.isFile() {
			return d, true
		}
	}
	return Destination{}, false
}

func loadIndexedDestinations(getenv func(string) string, environ []string) ([]Destination, error) {
	var dests []Destination
	for n := 1; getenv(fmt.Sprintf("DEST_%d_S3_BUCKET_PATH", n)) != "" || getenv(fmt.Sprintf("DEST_%d_FILE_PATH", n)) != ""; n++ {
		get := func(key string) string { return getenv(fmt.Sprintf("DEST_%d_%s", n, key)) }

		name := get("NAME")
		if name == "" {
			name = fmt.Sprintf("dest%d", n)
		}
		name = invalidNameChars.ReplaceAllString(name, "_")
		if name == LegacyDestinationName {
			return nil, fmt.Errorf("DEST_%d_NAME: %q is reserved for the S3_BUCKET_PATH destination", n, LegacyDestinationName)
		}

		if filePath := get("FILE_PATH"); filePath != "" {
			if get("S3_BUCKET_PATH") != "" {
				return nil, fmt.Errorf("DEST_%d: set either DEST_%d_S3_BUCKET_PATH or DEST_%d_FILE_PATH, not both", n, n, n)
			}
			if !filepath.IsAbs(filePath) {
				return nil, fmt.Errorf("DEST_%d_FILE_PATH: %q must be an absolute path", n, filePath)
			}
			info, err := os.Stat(filePath)
			if err != nil {
				return nil, fmt.Errorf("DEST_%d_FILE_PATH: %w", n, err)
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("DEST_%d_FILE_PATH: %q is not a directory", n, filePath)
			}
			dests = append(dests, Destination{Name: name, FilePath: filepath.Clean(filePath), index: n})
			continue
		}

		profile := get("AWS_PROFILE")
		if profile == "" {
			profile = "default"
		}

		d := Destination{
			Name:            name,
			BucketPath:      get("S3_BUCKET_PATH"),
			EndpointURL:     get("AWS_ENDPOINT_URL"),
			CredsFile:       get("AWS_CREDS_FILE"),
			Profile:         profile,
			AccessKeyID:     get("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: get("AWS_SECRET_ACCESS_KEY"),
			Region:          get("AWS_DEFAULT_REGION"),
			index:           n,
		}
		if d.CredsFile == "" && !d.hasEnvCreds() {
			return nil, fmt.Errorf("DEST_%d: either DEST_%d_AWS_CREDS_FILE or DEST_%d_AWS_ACCESS_KEY_ID, DEST_%d_AWS_SECRET_ACCESS_KEY and DEST_%d_AWS_DEFAULT_REGION are required", n, n, n, n, n)
		}
		if baseURL := get("S3_BASE_URL"); baseURL != "" {
			var err error
			if d.BaseURL, err = validateBaseURL(fmt.Sprintf("DEST_%d_S3_BASE_URL", n), baseURL); err != nil {
				return nil, err
			}
		}
		dests = append(dests, d)
	}

	// Catch gaps such as DEST_1 and DEST_3 without DEST_2, which would otherwise
	// silently drop a destination.
	var orphaned []int
	for _, kv := range environ {
		m := destVarPattern.FindStringSubmatch(kv)
		if m == nil {
			continue
		}
		if n, err := strconv.Atoi(m[1]); err == nil && (n < 1 || n > len(dests)) {
			orphaned = append(orphaned, n)
		}
	}
	if len(orphaned) > 0 {
		sort.Ints(orphaned)
		return nil, fmt.Errorf("DEST_%d_* variables are set but DEST_%d_S3_BUCKET_PATH or DEST_%d_FILE_PATH is missing (destinations must be numbered consecutively from 1)", orphaned[0], len(dests)+1, len(dests)+1)
	}

	return dests, nil
}

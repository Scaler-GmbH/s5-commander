package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// copyFileVerified copies src to dst through a temporary file in the target
// directory, verifies the written bytes against the source by SHA-1 and size,
// and only then renames the temporary file into place. It returns the SHA-1 of
// the source.
func copyFileVerified(src, dst string) (string, int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", 0, err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return "", 0, err
	}

	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}
	tmpName := filepath.Join(dir, fmt.Sprintf(".%s.tmp-%s", filepath.Base(dst), uuid.NewString()))
	tmp, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", 0, err
	}
	renamed := false
	defer func() {
		if !renamed {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	srcHash := sha1.New()
	written, err := io.Copy(tmp, io.TeeReader(in, srcHash))
	if err != nil {
		return "", 0, err
	}
	if err := tmp.Sync(); err != nil {
		return "", 0, err
	}
	if err := tmp.Close(); err != nil {
		return "", 0, err
	}
	srcSum := hex.EncodeToString(srcHash.Sum(nil))

	if written != info.Size() {
		return "", 0, fmt.Errorf("size mismatch copying %s: read %d bytes, expected %d", src, written, info.Size())
	}
	dstSum, dstSize, err := hashFile(tmpName)
	if err != nil {
		return "", 0, err
	}
	if dstSum != srcSum || dstSize != written {
		return "", 0, fmt.Errorf("integrity check failed copying %s to %s: sha1 %s (%d bytes) != %s (%d bytes)", src, dst, dstSum, dstSize, srcSum, written)
	}

	if err := os.Rename(tmpName, dst); err != nil {
		return "", 0, err
	}
	renamed = true
	if err := syncDir(dir); err != nil {
		return "", 0, err
	}
	return srcSum, written, nil
}

// hashFile returns the hex SHA-1 and size of the file at path.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha1.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

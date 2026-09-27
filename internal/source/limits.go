package source

import (
	"errors"
	"io"
	"os"
)

const (
	MaxArchiveBytes   int64  = 512 << 20
	MaxExpandedBytes  uint64 = 2 << 30
	MaxArchiveEntries        = 100_000
)

var (
	ErrArchiveTooLarge  = errors.New("source archive exceeds 512 MiB compressed size limit")
	ErrExpandedTooLarge = errors.New("source archive exceeds 2 GiB expanded size limit")
	ErrTooManyEntries   = errors.New("source archive exceeds 100000 entry limit")
)

// CopyArchive enforces the compressed-byte budget on the bytes actually read.
// The extra byte distinguishes an exact-limit archive from an oversized one.
func CopyArchive(dst io.Writer, src io.Reader, limit int64) (int64, error) {
	n, err := io.Copy(dst, io.LimitReader(src, limit+1))
	if n > limit {
		return n, ErrArchiveTooLarge
	}
	return n, err
}

func checkArchiveSize(path string, limit int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > limit {
		return ErrArchiveTooLarge
	}
	return nil
}

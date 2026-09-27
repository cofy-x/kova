package source

import (
	"archive/zip"
	"encoding/binary"
	"io"
	"os"
)

const (
	zipDirectoryHeaderSignature = 0x02014b50
	zipDirectoryEndSignature    = 0x06054b50
	zip64DirectoryEndSignature  = 0x06064b50
	zip64LocatorSignature       = 0x07064b50
)

// checkZipEntryCount reads the central directory without allocating one File
// per member. archive/zip materializes all Files before callers can check len.
func checkZipEntryCount(path string, maxEntries int) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	tailSize := min(size, int64(22+65_535))
	tail := make([]byte, tailSize)
	if _, err := file.ReadAt(tail, size-tailSize); err != nil {
		return err
	}
	var endOffset int64 = -1
	var end []byte
	for offset := len(tail) - 22; offset >= 0; offset-- {
		if binary.LittleEndian.Uint32(tail[offset:]) != zipDirectoryEndSignature {
			continue
		}
		commentSize := int(binary.LittleEndian.Uint16(tail[offset+20:]))
		if offset+22+commentSize <= len(tail) {
			endOffset = size - tailSize + int64(offset)
			end = tail[offset:]
			break
		}
	}
	if endOffset < 0 {
		return zip.ErrFormat
	}
	count := uint64(binary.LittleEndian.Uint16(end[10:]))
	directorySize := uint64(binary.LittleEndian.Uint32(end[12:]))
	directoryEnd := endOffset
	if count == 0xffff || directorySize == 0xffffffff || binary.LittleEndian.Uint32(end[16:]) == 0xffffffff {
		if endOffset < 20 {
			return zip.ErrFormat
		}
		var locator [20]byte
		if _, err := file.ReadAt(locator[:], endOffset-20); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(locator[:]) != zip64LocatorSignature {
			return zip.ErrFormat
		}
		zip64Offset := binary.LittleEndian.Uint64(locator[8:])
		if size < 56 || zip64Offset > uint64(size-56) {
			return zip.ErrFormat
		}
		var zip64End [56]byte
		if _, err := file.ReadAt(zip64End[:], int64(zip64Offset)); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(zip64End[:]) != zip64DirectoryEndSignature {
			return zip.ErrFormat
		}
		count = binary.LittleEndian.Uint64(zip64End[32:])
		directorySize = binary.LittleEndian.Uint64(zip64End[40:])
		directoryEnd = int64(zip64Offset)
	}
	if count > uint64(maxEntries) {
		return ErrTooManyEntries
	}
	if directorySize > uint64(directoryEnd) {
		return zip.ErrFormat
	}
	directoryStart := directoryEnd - int64(directorySize)
	reader := io.NewSectionReader(file, directoryStart, int64(directorySize))
	var found int
	for offset := int64(0); offset < int64(directorySize); {
		var header [46]byte
		if _, err := reader.ReadAt(header[:], offset); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(header[:]) != zipDirectoryHeaderSignature {
			return zip.ErrFormat
		}
		entryBytes := int64(46) + int64(binary.LittleEndian.Uint16(header[28:])) + int64(binary.LittleEndian.Uint16(header[30:])) + int64(binary.LittleEndian.Uint16(header[32:]))
		if entryBytes > int64(directorySize)-offset {
			return zip.ErrFormat
		}
		offset += entryBytes
		found++
		if found > maxEntries {
			return ErrTooManyEntries
		}
	}
	if uint64(found) != count {
		return zip.ErrFormat
	}
	return nil
}

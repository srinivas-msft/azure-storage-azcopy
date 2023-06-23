//go:build linux
// +build linux

package ste

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
)

// We use sendFile syscall in linux which copies the data between file discriptors
// Because this copying is done within the kernel, sendfile() is
// more efficient than the combination of read(2) and write(2),
// which would require transferring data to and from user space.
// https://man7.org/linux/man-pages/man2/sendfile.2.html
func copyFunc(ctx context.Context, size int64, src, dst string, dstFile io.WriteCloser) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()
	dst_fd := int((dstFile.(*os.File)).Fd())
	src_fd := int(source.Fd())
	var offset int64 = 0 //file offset
	var totalBytesWritten int64 = 0
	//max size that can be transfered using sendFile syscall is 2,147,479,552 bytes (~2.14GB)
	//So we divide our file into chunks of ~2.14GB and try to call the send
	var maxChunkSize int64 = 2147479552
	for size != 0 {
		var chunkSize int64 = 0
		if size <= maxChunkSize {
			chunkSize = size
			size = 0
		} else {
			chunkSize = maxChunkSize
			size -= maxChunkSize
		}
		written, err := syscall.Sendfile(dst_fd, src_fd, &offset, int(chunkSize))
		if err != nil {
			return err
		}
		if int64(written) != chunkSize {
			return errors.New("File Corrupted, Retry again")
		}
		totalBytesWritten += int64(written)
	}
	if totalBytesWritten != size {
		return errors.New("Bytes copied were less than the source size")
	}
	return nil
}

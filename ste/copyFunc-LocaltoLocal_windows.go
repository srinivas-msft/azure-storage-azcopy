//go:build windows
// +build windows

package ste

import (
	"context"
	"io"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32     = syscall.MustLoadDLL("kernel32.dll")
	copyFileProc = kernel32.MustFindProc("CopyFileW")
)

//CopyFile Win32API function which is aware of the underlying protocol such as SMB{2|3}	which makes it
//copying faster than io.Copy() which uses read and write syscall which would require transferring
//the data to and from the user space https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-copyfile

func copyFunc(ctx context.Context, size int64, src, dst string, dstFile io.WriteCloser) error {
	srcW := syscall.StringToUTF16(src)
	dstW := syscall.StringToUTF16(dst)

	rc, _, err := copyFileProc.Call(
		uintptr(unsafe.Pointer(&srcW[0])),
		uintptr(unsafe.Pointer(&dstW[0])),
		0)

	if rc == 0 {
		return &os.PathError{
			Op:   "CopyFile",
			Path: src,
			Err:  err,
		}
	}
	return nil
}

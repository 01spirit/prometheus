// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileutil

import (
	"os"
	"syscall"
	"unsafe"
)

/*
	内存映射：文件内容被直接映射到进程的虚拟地址空间，进程可以通过指针直接访问（读写文件内容），像访问内存中的变量一样
	传统的文件IO通常需要把数据从内核空间复制到用户空间的缓冲区，内存映射减少了这种复制操作，因为允许进程直接在用户空间操作文件数据
	多个进程可以映射同一个文件，通过内存映射的地址空间共享文件内容
	内存映射可以与异步IO一起使用来提升性能
如果文件被修改，可能需要将内存中的数据同步回文件
*/

// 传入文件和文件大小，返回映射的数据字节流
// 使用 Windows API 来创建一个文件映射，然后将文件的内容映射到内存中。这样，文件的内容就可以通过内存地址来访问，这通常比传统的文件 I/O 操作要快
// 传入文件和文件大小，int 在不同系统中有不同位数，64位系统是64位，32位系统是32位
func mmap(f *os.File, size int) ([]byte, error) {
	low, high := uint32(size), uint32(size>>32)                                                               // 分别存储文件大小的低四字节和高四字节，high 对于大于 4GB 的文件是必要的
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, high, low, nil) // 创建文件映射对象的句柄 h，创建失败时为 0
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size)) // 把文件映射到内存中，访问权限为只读
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	// 关闭文件映射对象的句柄
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	// 把映射的地址转换为字节流切片并返回
	return (*[maxMapSize]byte)(unsafe.Pointer(addr))[:size], nil
}

// 解除指定字节流代表的文件的内存映射
func munmap(b []byte) error {
	// 解除内存映射
	if err := syscall.UnmapViewOfFile((uintptr)(unsafe.Pointer(&b[0]))); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}

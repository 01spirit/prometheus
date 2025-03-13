// Copyright 2018 The Prometheus Authors
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
	"fmt"
	"os"
)

// MmapFile 内存映射的文件，MmapFile.f 是要映射的文件，MmapFile.b 是文件映射到内存中的数据字节流
type MmapFile struct {
	f *os.File // 要映射的文件
	b []byte   // 内存中的文件数据字节流
}

// OpenMmapFile 对路径为 path 的文件进行内存映射 mmap，获取其在内存中映射的数据字节流
func OpenMmapFile(path string) (*MmapFile, error) {
	return OpenMmapFileWithSize(path, 0) // size == 0 默认映射整个文件的内容
}

// OpenMmapFileWithSize 对路径为 path ，大小为 size 的文件进行内存映射 mmap，获取其在内存中映射的数据字节流
func OpenMmapFileWithSize(path string, size int) (mf *MmapFile, retErr error) {
	f, err := os.Open(path) // 打开文件
	if err != nil {
		return nil, fmt.Errorf("try lock file: %w", err)
	}
	defer func() { // 关闭文件
		if retErr != nil {
			f.Close()
		}
	}()
	if size <= 0 { // 设置默认文件大小
		info, err := f.Stat() // 文件的统计信息
		if err != nil {
			return nil, fmt.Errorf("stat: %w", err)
		}
		size = int(info.Size()) // 文件大小
	}

	b, err := mmap(f, size) // 对文件进行内存映射，返回其在内存中的字节流
	if err != nil {
		return nil, fmt.Errorf("mmap, size %d: %w", size, err)
	}

	// 把内存映射的结果构造成 MmapFile 结构
	return &MmapFile{f: f, b: b}, nil
}

func (f *MmapFile) Close() error {
	err0 := munmap(f.b) // 解除文件的内存映射
	err1 := f.f.Close() // 关闭文件

	if err0 != nil {
		return err0
	}
	return err1
}

func (f *MmapFile) File() *os.File {
	return f.f
}

func (f *MmapFile) Bytes() []byte {
	return f.b
}

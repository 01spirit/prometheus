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

// Package fileutil provides utility methods used when dealing with the filesystem in tsdb.
// It is largely copied from github.com/coreos/etcd/pkg/fileutil to avoid the
// dependency chain it brings with it.
// Please check github.com/coreos/etcd for licensing information.
package fileutil

import (
	"os"
	"path/filepath"
	"strings"
)

// CopyDirs copies all directories, subdirectories and files recursively including the empty folders.
// Source and destination must be full paths.
// 递归复制目录下的所有文件和空目录，参数必须是完整路径
func CopyDirs(src, dest string) error {
	// 创建目标目录
	if err := os.MkdirAll(dest, 0o777); err != nil {
		return err
	}
	files, err := readDirs(src) // 读取所有待复制文件的相对路径
	if err != nil {
		return err
	}

	for _, f := range files {
		dp := filepath.Join(dest, f) // 重新组织成绝对路径
		sp := filepath.Join(src, f)

		stat, err := os.Stat(sp) // 源目录下文件的统计信息
		if err != nil {
			return err
		}

		// Empty directories are also created.
		if stat.IsDir() { // 若是空目录
			if err := os.MkdirAll(dp, 0o777); err != nil {
				return err
			}
			continue
		}

		// 复制文件
		if err := copyFile(sp, dp); err != nil {
			return err
		}
	}
	return nil
}

// 从 src 向 dest 复制文件
func copyFile(src, dest string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	err = os.WriteFile(dest, data, 0o666)
	if err != nil {
		return err
	}
	return nil
}

// readDirs reads the source directory recursively and
// returns relative paths to all files and empty directories.
// 递归读取目录，返回其中所有文件和空目录的相对路径（去除目录路径前缀）
func readDirs(src string) ([]string, error) {
	var files []string

	// 遍历目录
	err := filepath.Walk(src, func(path string, f os.FileInfo, err error) error {
		relativePath := strings.TrimPrefix(path, src)
		if len(relativePath) > 0 {
			files = append(files, relativePath)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// Rename safely renames a file.
// 重命名文件
func Rename(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	// 把修改保存到磁盘
	if err = pdir.Sync(); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
}

// Replace moves a file or directory to a new location and deletes any previous data.
// It is not atomic.
// 把文件或目录移动到新的位置，删除原来的数据
func Replace(from, to string) error {
	// Remove destination only if it is a dir otherwise leave it to os.Rename
	// as it replaces the destination file and is atomic.
	{
		f, err := os.Stat(to)
		if !os.IsNotExist(err) {
			if err == nil && f.IsDir() {
				if err := os.RemoveAll(to); err != nil {
					return err
				}
			}
		}
	}

	return Rename(from, to)
}

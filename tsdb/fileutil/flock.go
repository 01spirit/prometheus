// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileutil

import (
	"os"
	"path/filepath"
)

// Releaser provides the Release method to release a file lock.
// 释放文件的锁
type Releaser interface {
	Release() error
}

// Flock locks the file with the provided name. If the file does not exist, it is
// created. The returned Releaser is used to release the lock. existed is true
// if the file to lock already existed. A non-nil error is returned if the
// locking has failed. Neither this function nor the returned Releaser is
// goroutine-safe.
// 给指定的文件加锁，若文件不存在，创建；返回用于释放锁的 Releaser，若要加锁的文件已经存在， existed = true；若加锁失败返回异常
func Flock(fileName string) (r Releaser, existed bool, err error) {
	// 若文件不存在，创建；若存在，没有影响，继续
	if err = os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
		return nil, false, err
	}

	// 判断文件是否存在
	_, err = os.Stat(fileName)
	existed = err == nil

	// 为文件加锁
	r, err = newLock(fileName)
	return r, existed, err
}

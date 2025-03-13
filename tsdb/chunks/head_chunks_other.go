// Copyright 2020 The Prometheus Authors
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

//go:build !windows

package chunks

// HeadChunkFilePreallocationSize is the size to which the m-map file should be preallocated when a new file is cut.
// Windows needs pre-allocations while the other OS does not. But we observed that a 0 pre-allocation causes unit tests to flake.
// This small allocation for non-Windows OSes removes the flake.
// 定义新切分的内存映射文件（HeadChunk）应该预分配的大小
// 在 Windows 操作系统上，创建文件时通常需要预分配空间，而其他操作系统（如 Linux 或 macOS）则不需要。预分配可以减少文件在写入过程中的扩展操作，因为文件系统会提前为文件分配足够的空间，从而提高写入性能。
// 如果不进行预分配（即预分配大小为 0），会导致单元测试出现不稳定的情况（flake）.因此，即使是在不需要预分配的操作系统上，也进行一个小的预分配，可以消除这种不稳定性。
// 128 KB
var HeadChunkFilePreallocationSize int64 = MinWriteBufferSize * 2

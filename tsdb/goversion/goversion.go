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

//go:build go1.12

// 上面这行是 构建标签 build tags，在源代码文件中以注释的形式出现，或在命令行中以参数的形式提供，告诉编译器在编译时要满足的条件
// 这里要求 go 的版本在 1.12 及以上才能编译这段代码，否则会跳过，可能会报错，运行时无影响

// Package goversion enforces the go version supported by the tsdb module.
package goversion

const _SoftwareRequiresGOVERSION1_12 = uint8(0)

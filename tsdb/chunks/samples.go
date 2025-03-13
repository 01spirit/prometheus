// Copyright 2023 The Prometheus Authors
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

package chunks

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Samples 是 Sample 数组的接口
type Samples interface {
	Get(i int) Sample // 获取第 i 个 Sample
	Len() int         // 获取 Sample 数量
}

// Sample 采集的样本数据，
type Sample interface {
	T() int64                      // 时间戳
	F() float64                    // 浮点值
	H() *histogram.Histogram       // 直方图类型的值
	FH() *histogram.FloatHistogram // 浮点直方图类型的值
	Type() chunkenc.ValueType      // 值的类型，四种（无类型、浮点、两种直方图）
}

// SampleSlice 样本数据切片，实现了 Samples 接口
type SampleSlice []Sample

func (s SampleSlice) Get(i int) Sample { return s[i] }
func (s SampleSlice) Len() int         { return len(s) }

// 实现了 Sample 接口
type sample struct {
	t  int64
	f  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) F() float64 {
	return s.f
}

func (s sample) H() *histogram.Histogram {
	return s.h
}

func (s sample) FH() *histogram.FloatHistogram {
	return s.fh
}

// Type 获取样本数据类型，直方图或 float
func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

// GenerateSamples starting at start and counting up numSamples.
// 生成值从 start 开始的 numSamples 个 Sample，返回其数组
func GenerateSamples(start, numSamples int) []Sample {
	return generateSamples(start, numSamples, func(i int) Sample {
		return sample{ // 用 i 构造一个 Sample
			t: int64(i),
			f: float64(i),
		}
	})
}

// 根据输入参数和函数生成 Sample 数组
func generateSamples(start, numSamples int, gen func(int) Sample) []Sample {
	samples := make([]Sample, 0, numSamples)
	for i := start; i < start+numSamples; i++ {
		samples = append(samples, gen(i)) // gen(i) 用 i 构造一个 Sample，t 和 f 都是 i
	}
	return samples
}

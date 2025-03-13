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

package chunkenc

import (
	"fmt"
	"math"
	"sync"

	"github.com/prometheus/prometheus/model/histogram"
)

// Encoding is the identifier for a chunk encoding.  chunk的编码方式，如下4个常量（none、XOR（float），两种直方图数据编码）
type Encoding uint8

// The different available chunk encodings.
const (
	EncNone Encoding = iota
	EncXOR           // float 编码方式
	EncHistogram
	EncFloatHistogram
)

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	case EncHistogram:
		return "histogram"
	case EncFloatHistogram:
		return "floathistogram"
	}
	return "<unknown>"
}

// IsValidEncoding returns true for supported encodings.	编码方式是否有效
func IsValidEncoding(e Encoding) bool {
	return e == EncXOR || e == EncHistogram || e == EncFloatHistogram
}

const (
	// MaxBytesPerXORChunk is the maximum size an XOR chunk can be.	采用 XOR 编码的 chunk 最多有 1024 bytes
	MaxBytesPerXORChunk = 1024
	// TargetBytesPerHistogramChunk sets a size target for each histogram chunk.
	TargetBytesPerHistogramChunk = 1024
	// MinSamplesPerHistogramChunk sets a minimum sample count for histogram chunks. This is desirable because a single
	// histogram sample can be larger than TargetBytesPerHistogramChunk but we want to avoid too-small sample count
	// chunks so we can achieve some measure of compression advantage even while dealing with really large histograms.
	// Note that this minimum sample count is not enforced across chunk range boundaries (for example, if the chunk
	// range is 100 and the first sample in the chunk range is 99, the next sample will be included in a new chunk
	// resulting in the old chunk containing only a single sample).
	MinSamplesPerHistogramChunk = 10
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
// Chunk 中存储了一系列 sample pair，可以迭代或添加
type Chunk interface {
	// Iterable 接口, chunk 需要实现其中的迭代器方法
	Iterable

	// Bytes returns the underlying byte slice of the chunk.	返回 chunk 中的字节数据
	Bytes() []byte

	// Encoding returns the encoding type of the chunk.	chunk 的编码方式（XOR、直方图）
	Encoding() Encoding

	// Appender returns an appender to append samples to the chunk.		添加器,向chunk的流的末尾添加数据
	Appender() (Appender, error)

	// NumSamples returns the number of samples in the chunk.	chunk 中的 sample 数量
	NumSamples() int

	// Compact is called whenever a chunk is expected to be complete (no more	chunk 中不再添加数据之后进行压缩
	// samples appended) and the underlying implementation can eventually
	// optimize the chunk.
	// There's no strong guarantee that no samples will be appended once
	// Compact() is called. Implementing this function is optional.
	Compact()

	// Reset resets the chunk given stream.	用给定的字节流重置 chunk
	Reset(stream []byte)
}

type Iterable interface {
	// The iterator passed as argument is for re-use.
	// Depending on implementation, the iterator can
	// be re-used or a new iterator can be allocated.  迭代器接口
	Iterator(Iterator) Iterator
}

// Appender adds sample pairs to a chunk.	添加器，向 chunk 中添加 sample
type Appender interface {
	Append(int64, float64)

	// AppendHistogram and AppendFloatHistogram append a histogram sample to a histogram or float histogram chunk.
	// Appending a histogram may require creating a completely new chunk or recoding (changing) the current chunk.
	// The Appender prev is used to determine if there is a counter reset between the previous Appender and the current Appender.
	// The Appender prev is optional and only taken into account when the first sample is being appended.
	// The bool appendOnly governs what happens when a sample cannot be appended to the current chunk. If appendOnly is true, then
	// in such case an error is returned without modifying the chunk. If appendOnly is false, then a new chunk is created or the
	// current chunk is recoded to accommodate the sample.
	// The returned Chunk c is nil if sample could be appended to the current Chunk, otherwise c is the new Chunk.
	// The returned bool isRecoded can be used to distinguish between the new Chunk c being a completely new Chunk
	// or the current Chunk recoded to a new Chunk.
	// The Appender app that can be used for the next append is always returned.
	AppendHistogram(prev *HistogramAppender, t int64, h *histogram.Histogram, appendOnly bool) (c Chunk, isRecoded bool, app Appender, err error)
	AppendFloatHistogram(prev *FloatHistogramAppender, t int64, h *histogram.FloatHistogram, appendOnly bool) (c Chunk, isRecoded bool, app Appender, err error)
}

// Iterator is a simple iterator that can only get the next value.
// Iterator iterates over the samples of a time series, in timestamp-increasing order.	迭代器，按时间升序迭代 TS 中的 sample
// 要实现 Iterable 接口的话，首先要实现 iterator 接口，创建迭代器类型
type Iterator interface {
	// Next advances the iterator by one and returns the type of the value
	// at the new position (or ValNone if the iterator is exhausted).	迭代器前进一位，返回新的位置的值的类型（float、histogram）
	Next() ValueType
	// Seek advances the iterator forward to the first sample with a
	// timestamp equal or greater than t. If the current sample found by a
	// previous `Next` or `Seek` operation already has this property, Seek
	// has no effect. If a sample has been found, Seek returns the type of
	// its value. Otherwise, it returns ValNone, after which the iterator is
	// exhausted.																迭代找到时间戳 >= 给定值的首个 sample；若当前时间戳满足条件，无影响；找到之后返回值的类型；迭代到末尾没找到就返回 ValNone 类型
	Seek(t int64) ValueType
	// At returns the current timestamp/value pair if the value is a float.
	// Before the iterator has advanced, the behaviour is unspecified.		若当前值是 float，返回当前 timestamp-value pair
	At() (int64, float64)
	// AtHistogram returns the current timestamp/value pair if the value is a
	// histogram with integer counts. Before the iterator has advanced, the behaviour
	// is unspecified.
	// The method accepts an optional Histogram object which will be
	// reused when not nil. Otherwise, a new Histogram object will be allocated.
	AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram)
	// AtFloatHistogram returns the current timestamp/value pair if the
	// value is a histogram with floating-point counts. It also works if the
	// value is a histogram with integer counts, in which case a
	// FloatHistogram copy of the histogram is returned. Before the iterator
	// has advanced, the behaviour is unspecified.
	// The method accepts an optional FloatHistogram object which will be
	// reused when not nil. Otherwise, a new FloatHistogram object will be allocated.
	AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram)
	// AtT returns the current timestamp.
	// Before the iterator has advanced, the behaviour is unspecified.		返回当前时间戳
	AtT() int64
	// Err returns the current error. It should be used only after the
	// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
	Err() error
}

// ValueType defines the type of a value an Iterator points to.	迭代器指向的值的类型，四种（None、float、两种histogram）
type ValueType uint8

// Possible values for ValueType.
const (
	ValNone           ValueType = iota // No value at the current position.
	ValFloat                           // A simple float, retrieved with At.
	ValHistogram                       // A histogram, retrieve with AtHistogram, but AtFloatHistogram works, too.
	ValFloatHistogram                  // A floating-point histogram, retrieve with AtFloatHistogram.
)

func (v ValueType) String() string {
	switch v {
	case ValNone:
		return "none"
	case ValFloat:
		return "float"
	case ValHistogram:
		return "histogram"
	case ValFloatHistogram:
		return "floathistogram"
	default:
		return "unknown"
	}
}

// ChunkEncoding 返回该类型值对应的编码方式
func (v ValueType) ChunkEncoding() Encoding {
	switch v {
	case ValFloat:
		return EncXOR
	case ValHistogram:
		return EncHistogram
	case ValFloatHistogram:
		return EncFloatHistogram
	default:
		return EncNone
	}
}

// NewChunk 初始化一个该类型值对应的 chunk
func (v ValueType) NewChunk() (Chunk, error) {
	switch v {
	case ValFloat:
		return NewXORChunk(), nil
	case ValHistogram:
		return NewHistogramChunk(), nil
	case ValFloatHistogram:
		return NewFloatHistogramChunk(), nil
	default:
		return nil, fmt.Errorf("value type %v unsupported", v)
	}
}

// MockSeriesIterator returns an iterator for a mock series with custom timeStamps and values.	用假数据模拟的迭代器
func MockSeriesIterator(timestamps []int64, values []float64) Iterator {
	return &mockSeriesIterator{
		timeStamps: timestamps,
		values:     values,
		currIndex:  -1,
	}
}

// 模拟 Samples数组，和迭代器
type mockSeriesIterator struct {
	timeStamps []int64
	values     []float64
	currIndex  int
}

// 模拟数据，无类型
func (it *mockSeriesIterator) Seek(int64) ValueType { return ValNone }

// 迭代器 currIndex 指向的 sample 数据
func (it *mockSeriesIterator) At() (int64, float64) {
	return it.timeStamps[it.currIndex], it.values[it.currIndex]
}

func (it *mockSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return math.MinInt64, nil
}

func (it *mockSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return math.MinInt64, nil
}

// 当前的时间戳
func (it *mockSeriesIterator) AtT() int64 {
	return it.timeStamps[it.currIndex]
}

// 迭代器前移，返回当前值的类型，若结束返回 none
func (it *mockSeriesIterator) Next() ValueType {
	if it.currIndex < len(it.timeStamps)-1 {
		it.currIndex++
		return ValFloat
	}

	return ValNone
}
func (it *mockSeriesIterator) Err() error { return nil }

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

// 无操作，返回无效的默认值，在没有实际数据或不需要具体操作时使用
type nopIterator struct{}

func (nopIterator) Next() ValueType      { return ValNone }
func (nopIterator) Seek(int64) ValueType { return ValNone }
func (nopIterator) At() (int64, float64) { return math.MinInt64, 0 }
func (nopIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return math.MinInt64, nil
}

func (nopIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return math.MinInt64, nil
}
func (nopIterator) AtT() int64 { return math.MinInt64 }
func (nopIterator) Err() error { return nil }

// Pool is used to create and reuse chunk references to avoid allocations.
// 创建和复用 chunk reference，避免分配操作
type Pool interface {
	Put(Chunk) error
	Get(e Encoding, b []byte) (Chunk, error)
}

// pool is a memory pool of chunk objects.	chunk 对象的内存池，实现了 Pool 接口
type pool struct {
	xor            sync.Pool
	histogram      sync.Pool
	floatHistogram sync.Pool
}

// NewPool returns a new pool.	Pool中存放了 chunk 的地址
func NewPool() Pool {
	return &pool{
		xor: sync.Pool{
			New: func() interface{} {
				return &XORChunk{b: bstream{}}
			},
		},
		histogram: sync.Pool{
			New: func() interface{} {
				return &HistogramChunk{b: bstream{}}
			},
		},
		floatHistogram: sync.Pool{
			New: func() interface{} {
				return &FloatHistogramChunk{b: bstream{}}
			},
		},
	}
}

// Get 根据编码方式获取相应的 chunk 的地址，用指定字节流重置 chunk
func (p *pool) Get(e Encoding, b []byte) (Chunk, error) {
	var c Chunk
	switch e {
	case EncXOR:
		c = p.xor.Get().(*XORChunk)
	case EncHistogram:
		c = p.histogram.Get().(*HistogramChunk)
	case EncFloatHistogram:
		c = p.floatHistogram.Get().(*FloatHistogramChunk)
	default:
		return nil, fmt.Errorf("invalid chunk encoding %q", e)
	}

	c.Reset(b)
	return c, nil
}

// Put 把 chunk 放入对应的 pool 中
func (p *pool) Put(c Chunk) error {
	var sp *sync.Pool
	var ok bool
	switch c.Encoding() {
	case EncXOR:
		_, ok = c.(*XORChunk)
		sp = &p.xor
	case EncHistogram:
		_, ok = c.(*HistogramChunk)
		sp = &p.histogram
	case EncFloatHistogram:
		_, ok = c.(*FloatHistogramChunk)
		sp = &p.floatHistogram
	default:
		return fmt.Errorf("invalid chunk encoding %q", c.Encoding())
	}
	if !ok {
		// This may happen often with wrapped chunks. Nothing we can really do about
		// it but returning an error would cause a lot of allocations again. Thus,
		// we just skip it.
		return nil
	}

	c.Reset(nil)
	sp.Put(c)
	return nil
}

// FromData returns a chunk from a byte slice of chunk data.
// This is there so that users of the library can easily create chunks from bytes.
// 用指定的编码方式和字节流创建一个 chunk
func FromData(e Encoding, d []byte) (Chunk, error) {
	switch e {
	case EncXOR:
		return &XORChunk{b: bstream{count: 0, stream: d}}, nil
	case EncHistogram:
		return &HistogramChunk{b: bstream{count: 0, stream: d}}, nil
	case EncFloatHistogram:
		return &FloatHistogramChunk{b: bstream{count: 0, stream: d}}, nil
	}
	return nil, fmt.Errorf("invalid chunk encoding %q", e)
}

// NewEmptyChunk returns an empty chunk for the given encoding.	用给定的编码方式创建一个新 chunk
func NewEmptyChunk(e Encoding) (Chunk, error) {
	switch e {
	case EncXOR:
		return NewXORChunk(), nil
	case EncHistogram:
		return NewHistogramChunk(), nil
	case EncFloatHistogram:
		return NewFloatHistogramChunk(), nil
	}
	return nil, fmt.Errorf("invalid chunk encoding %q", e)
}

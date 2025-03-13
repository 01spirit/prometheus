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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It was modified to accommodate reading from byte slices without modifying
// the underlying bytes, which would panic when reading from mmapped
// read-only byte slices.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/prometheus/prometheus/model/histogram"
)

const (
	chunkCompactCapacityThreshold = 32 // 触发压缩的界限
)

// XORChunk holds XOR encoded sample data.	float sample 存储在 XORChunk 中，内部是编码后的字节流
// 实现了 Chunk 接口，Iterable 和 Iterator 接口，Appender 接口
type XORChunk struct {
	b bstream
}

// NewXORChunk returns a new chunk with XOR encoding.	创建一个 XOR 编码方式的 chunk，返回其地址
func NewXORChunk() *XORChunk {
	b := make([]byte, 2, 128) // 容量 128，初始长度 2 的字节流；chunk 中的前两字节存放 sample 的数量
	return &XORChunk{b: bstream{stream: b, count: 0}}
}

func (c *XORChunk) Reset(stream []byte) {
	c.b.Reset(stream)
}

// Encoding returns the encoding type.	编码方式，uint8, 1 byte
func (c *XORChunk) Encoding() Encoding {
	return EncXOR
}

// Bytes returns the underlying byte slice of the chunk.	返回字节流
func (c *XORChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.	XORChunk 中的数据 Sample 数量
func (c *XORChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes())) // XORChunk 结构中，起始 2 byte 是 uint16 的 NumSamples，Uint16() 截取这两字节并转化
}

// Compact implements the Chunk interface.	实现 Compact 接口，没用到，不用管具体实现
func (c *XORChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

// Appender implements the Chunk interface.
// It is not valid to call Appender() multiple times concurrently or to use multiple
// Appenders on the same chunk.
// 获取添加器，向 XORChunk 末尾添加数据，添加器中保存了当前 chunk 末尾数据的值和元数据
func (c *XORChunk) Appender() (Appender, error) {
	it := c.iterator(nil) // 为 chunk 分配一个迭代器

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	for it.Next() != ValNone { // 遍历到迭代器中数据的末尾
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	a := &xorAppender{ // 用迭代器的数据初始化一个 添加器
		b:        &c.b,
		t:        it.t,
		v:        it.val,
		tDelta:   it.tDelta,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if it.numTotal == 0 { // 若当前无数据，前导零数量 leading 置为 全1
		a.leading = 0xff
	}
	return a, nil
}

// xorIterator 实现，获取指向 chunk 数据头部的迭代器
func (c *XORChunk) iterator(it Iterator) *xorIterator {
	if xorIter, ok := it.(*xorIterator); ok { // 复用迭代器 it，重置 xorIter.Reader 为 c.bstream 的数据的起始字节
		xorIter.Reset(c.b.bytes())
		return xorIter
	}
	return &xorIterator{ // 传入的 iterator 为空，分配空间并初始化
		// The first 2 bytes contain chunk headers.
		// We skip that for actual samples.
		br:       newBReader(c.b.bytes()[2:]),          // 跳过 XORChunk 中的前两字节元数据
		numTotal: binary.BigEndian.Uint16(c.b.bytes()), // 获取 XORChunk 中的 Sample 数量
		t:        math.MinInt64,
	}
}

// Iterator implements the Chunk interface.
// Iterator() must not be called concurrently with any modifications to the chunk,
// but after it returns you can use an Iterator concurrently with an Appender or
// other Iterators.
// 实现了 Iterable 接口， it 用于复用迭代器，若为空，分配一个新的迭代器
func (c *XORChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

type xorAppender struct {
	b *bstream // XORChunk.bstream

	// 以下信息来自迭代器读取的流中的最后一条数据
	t      int64   // 时间戳
	v      float64 // 完整值
	tDelta uint64  // 相较于前一时间戳的差值

	leading  uint8 // 值的异或delta的前导零数量
	trailing uint8 // 值的尾随零的数量
}

// Append 向 chunk 中添加一条 Sample：向 XORChunk.bstream 的末尾写入一条数据 timestamp-value pair
func (a *xorAppender) Append(t int64, v float64) {
	var tDelta uint64
	num := binary.BigEndian.Uint16(a.b.bytes()) // 流的前两字节表示流中的 Sample 数量
	switch num {
	// 根据流中数据量处理
	case 0: // 首次写入
		buf := make([]byte, binary.MaxVarintLen64)         // 10字节的数组，可以容纳所有编码数据
		for _, b := range buf[:binary.PutVarint(buf, t)] { // 把时间戳编码并写入 buffer，依次向流中写入编码字节
			a.b.writeByte(b)
		}
		a.b.writeBits(math.Float64bits(v), 64) // 把值编码成 uint64，完整写入流中
	case 1: // 写入第二条数据
		tDelta = uint64(t - a.t) // 计算相较于前一条时间戳的差值

		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] { // 向流中写入编码的时间戳差值
			a.b.writeByte(b)
		}

		a.writeVDelta(v) // 向流中写入值的异或编码
	default: // 写入第三条及以后的数据
		tDelta = uint64(t - a.t)        // 计算相较于前一条时间戳的差值
		dod := int64(tDelta - a.tDelta) // 计算差值的差值

		// Gorilla has a max resolution of seconds, Prometheus milliseconds.
		// Thus we use higher value range steps with larger bit size.
		//
		// TODO(beorn7): This seems to needlessly jump to large bit
		// sizes even for very small deviations from zero. Timestamp
		// compression can probably benefit from some smaller bit
		// buckets. See also what was done for histogram encoding in
		// varbit.go.
		switch {
		case dod == 0: // 时间差值相等，写入 bit 0
			a.b.writeBit(zero)
		case bitRange(dod, 14): // dod 能用 14 bit 表示，编码用 0b10 开头
			a.b.writeBits(0b10, 2)
			a.b.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			a.b.writeBits(0b110, 3)
			a.b.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			a.b.writeBits(0b1110, 4)
			a.b.writeBits(uint64(dod), 20)
		default: // 只能用 64 bit 完整表示
			a.b.writeBits(0b1111, 4)
			a.b.writeBits(uint64(dod), 64)
		}

		a.writeVDelta(v) // 写入异或值
	}

	a.t = t
	a.v = v
	binary.BigEndian.PutUint16(a.b.bytes(), num+1) // 添加一条 Sample之后，修改流中的前两字节 numSamples
	a.tDelta = tDelta                              // 更新添加器的元数据，零的信息在写入值时已经更新了
}

// bitRange returns whether the given integer can be represented by nbits.
// See docs/bstream.md.
// 判断给定 int64 能否用 nbits 个 bit 表示，应该是从右侧有多少个有效位
func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

// 向流中写入数据 v 的异或值
func (a *xorAppender) writeVDelta(v float64) {
	xorWrite(a.b, v, a.v, &a.leading, &a.trailing)
}

func (a *xorAppender) AppendHistogram(*HistogramAppender, int64, *histogram.Histogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a histogram sample to a float chunk")
}

func (a *xorAppender) AppendFloatHistogram(*FloatHistogramAppender, int64, *histogram.FloatHistogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a float histogram sample to a float chunk")
}

// 实现了 Iterator 接口，XORChunk 的迭代器
type xorIterator struct {
	br       bstreamReader // 字节流读取器
	numTotal uint16        // chunk 中的 Sample 总数
	numRead  uint16        // 已经读取的 Sample 数量

	t   int64   // 迭代器当前数据的时间戳
	val float64 // 当前数据的值

	leading  uint8 // 差值中前导零 bit 0 的数量
	trailing uint8 // 差值中尾随零 bit 0 的数量

	tDelta uint64 // 当前时间戳相比于前一个的差值
	err    error
}

// Seek 用迭代器找到时间戳对应的数据，返回其数据类型
func (it *xorIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}

	for t > it.t || it.numRead == 0 { // 目标时间大于迭代器的当前时间 或 迭代器中已经读取 Sample 数量为 0，读取 Next 或 返回 None
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValFloat
}

// At 迭代器当前指向的数据的时间戳和值
func (it *xorIterator) At() (int64, float64) {
	return it.t, it.val
}

func (it *xorIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("cannot call xorIterator.AtHistogram")
}

func (it *xorIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("cannot call xorIterator.AtFloatHistogram")
}

// 时间戳
func (it *xorIterator) AtT() int64 {
	return it.t
}

func (it *xorIterator) Err() error {
	return it.err
}

// Reset 用传入的字节流重置迭代器，设置其 bstreamReader 和 元数据
func (it *xorIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	// 前两个 byte 是 NumSamples ，跳过不读
	it.br = newBReader(b[2:])                // 从 XORChunk 首个字节开始的 Reader
	it.numTotal = binary.BigEndian.Uint16(b) // chunk 中的 Sample 总数

	it.numRead = 0
	it.t = 0
	it.val = 0
	it.leading = 0
	it.trailing = 0
	it.tDelta = 0
	it.err = nil
}

// Next 迭代读取下一条数据，相关数据存入迭代器的元素中，返回其数据类型
func (it *xorIterator) Next() ValueType {
	if it.err != nil || it.numRead == it.numTotal { // 全部读完
		return ValNone
	}

	if it.numRead == 0 { // 还没读过数据，则流中存储的是完整的时间戳，不是差值，可以直接转换出来
		t, err := binary.ReadVarint(&it.br) // 从 bstreamReader 读取时间戳
		if err != nil {
			it.err = err
			return ValNone
		}
		v, err := it.br.readBits(64) // 从流中读取 64 bit 值，读取结果表示为 uint64
		if err != nil {
			it.err = err
			return ValNone
		}
		it.t = t
		it.val = math.Float64frombits(v) // 把 uint64 的读取结果转换为 float64 类型的 值

		it.numRead++ // 已经读取的 Sample 数量
		return ValFloat
	}
	if it.numRead == 1 { // 已经读了一条数据，则第二条时间戳以差值 delta 的形式存储
		tDelta, err := binary.ReadUvarint(&it.br) // 读取差值形式表示的时间戳
		if err != nil {
			it.err = err
			return ValNone
		}
		it.tDelta = tDelta       // 存入迭代器
		it.t += int64(it.tDelta) // 计算出当前数据的时间戳

		return it.readValue() // 读取值
	}

	var d byte
	// read delta-of-delta
	// 第三条时间戳开始，以 差值的差值 的形式存储
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBitFast() // 逐个 bit 读取
		if err != nil {                 // 取决于 read buffer 中是否有可读 bit，调用不同函数
			bit, err = it.br.readBit()
		}
		if err != nil {
			it.err = err
			return ValNone
		}
		if bit == zero { // 读到 0 退出，把 1 存入 d
			break
		}
		d |= 1
	}
	var sz uint8 // 经过压缩后，不定长编码的时间戳 bit 长度不同，sz 是需要读取的 bit 数量
	var dod int64
	switch d {
	case 0b0:
	// 差值的差值 为 0
	// dod == 0
	case 0b10:
		sz = 14
	case 0b110:
		sz = 17
	case 0b1110:
		sz = 20
	case 0b1111:
		// Do not use fast because it's very unlikely it will succeed.
		bits, err := it.br.readBits(64) // 读取完整的时间戳差值
		if err != nil {
			it.err = err
			return ValNone
		}

		dod = int64(bits)
	}

	if sz != 0 { // 时间戳以 dod 形式存储，从流中读取 sz 个 bit，转化为 dod  形式的时间戳
		bits, err := it.br.readBitsFast(sz) // sz 的大小远小于 buffer 的长度 64，尝试直接读取 buffer 加快速度
		if err != nil {
			bits, err = it.br.readBits(sz)
		}
		if err != nil {
			it.err = err
			return ValNone
		}

		// Account for negative numbers, which come back as high unsigned numbers.
		// See docs/bstream.md.
		if bits > (1 << (sz - 1)) { // 编码方式，详见文档
			bits -= 1 << sz
		}
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod) // 当前数据的时间戳相较于前一个的差值
	it.t += int64(it.tDelta)                   // 当前时间戳

	return it.readValue() // 读取值
}

// 读取值，返回值的数据类型
func (it *xorIterator) readValue() ValueType {
	err := xorRead(&it.br, &it.val, &it.leading, &it.trailing)
	if err != nil {
		it.err = err
		return ValNone
	}
	it.numRead++ // 读取的 Sample 数量
	return ValFloat
}

// 以异或值形式向流中写入数据 newValue
func xorWrite(b *bstream, newValue, currentValue float64, leading, trailing *uint8) {
	delta := math.Float64bits(newValue) ^ math.Float64bits(currentValue) // 异或 计算待写入数据和当前数据的 差值（uint64）

	if delta == 0 { // 相等，直接写入 bit 0，结束
		b.writeBit(zero)
		return
	}
	b.writeBit(one) // 不相等，写入 bit 1，表示需要进一步写入

	newLeading := uint8(bits.LeadingZeros64(delta))   // 差值中的 前导零 数量
	newTrailing := uint8(bits.TrailingZeros64(delta)) // 差值中的 尾随零 的数量

	// Clamp number of leading zeros to avoid overflow when encoding.
	// 限制 前导零 的数量，避免编码时溢出，最多 31 bit
	if newLeading >= 32 {
		newLeading = 31
	}

	// leading 不全为 1（全为1表示之前没有数据，第一次插入） && 新的前导零和尾随零的数量都不少于前一条数据的，保持前一条数据的零的表示不变，写入一个 bit 0，然后写入差值数据
	if *leading != 0xff && newLeading >= *leading && newTrailing >= *trailing {
		// In this case, we stick with the current leading/trailing.
		b.writeBit(zero)
		b.writeBits(delta>>*trailing, 64-int(*leading)-int(*trailing)) // delta 的有效数据移到最右侧，向流中写入 delta 的有效位
		return
	}

	// Update leading/trailing for the caller.
	// 前导零或尾随零数量减少了，更新迭代器的元数据
	*leading, *trailing = newLeading, newTrailing

	b.writeBit(one)                    // 写入 bit 1，表示零的数量有变化
	b.writeBits(uint64(newLeading), 5) // 5 bit 前导零的数量

	// Note that if newLeading == newTrailing == 0, then sigbits == 64. But
	// that value doesn't actually fit into the 6 bits we have.  Luckily, we
	// never need to encode 0 significant bits, since that would put us in
	// the other case (vdelta == 0).  So instead we write out a 0 and adjust
	// it back to 64 on unpacking.
	sigbits := 64 - newLeading - newTrailing
	b.writeBits(uint64(sigbits), 6)               // 6 bit 有效位 的数量，0 表示 64 位均有效	； 尾随零的数量可以用 64 - leading - sigbits 计算，不存储，节省空间
	b.writeBits(delta>>newTrailing, int(sigbits)) // 向流中写入 delta 的有效位
}

// 读取异或编码存储的 value ，解码，存入 iterator
func xorRead(br *bstreamReader, value *float64, leading, trailing *uint8) error {
	bit, err := br.readBitFast() // 从流中读取一个 bit，取决于 buffer 的可读位数调用不同方法
	if err != nil {
		bit, err = br.readBit()
	}
	if err != nil {
		return err
	}
	if bit == zero { // 首位为 0，这一位表示相邻两条数据是否相等，0 表示该数据和前一条数据相等，迭代器中的元数据不需要改变
		return nil
	}
	bit, err = br.readBitFast() // 读第二个 bit，这一位表示前导零和尾随零的数量是否能用前一条数据的表示（该数据的两侧均有更多 0）。0 表示复用前一条数据的
	if err != nil {
		bit, err = br.readBit()
	}
	if err != nil {
		return err
	}

	var (
		bits                           uint64
		newLeading, newTrailing, mbits uint8
	)

	if bit == zero { // 第二位是 0，复用首尾 bit 0 的个数
		// Reuse leading/trailing zero bits.
		newLeading, newTrailing = *leading, *trailing
		mbits = 64 - newLeading - newTrailing // 有效位数
	} else {
		bits, err = br.readBitsFast(5) // 读第 3 到 7 bit，表示前导零的数量
		if err != nil {
			bits, err = br.readBits(5)
		}
		if err != nil {
			return err
		}
		newLeading = uint8(bits) // 前导零的数量

		bits, err = br.readBitsFast(6) // 读第 8 到 13 bit，表示有效位的数量
		if err != nil {
			bits, err = br.readBits(6)
		}
		if err != nil {
			return err
		}
		mbits = uint8(bits) // 有效位的数量
		// 0 significant bits here means we overflowed and we actually
		// need 64; see comment in xrWrite.
		if mbits == 0 { // 0 实际上是 64 bit 有效位
			mbits = 64
		}
		newTrailing = 64 - newLeading - mbits // 尾随零的数量
		// Update leading/trailing zero bits for the caller.
		*leading, *trailing = newLeading, newTrailing // 更新迭代器的元数据
	}
	bits, err = br.readBitsFast(mbits) // 读取有效位
	if err != nil {
		bits, err = br.readBits(mbits)
	}
	if err != nil {
		return err
	}
	vbits := math.Float64bits(*value)    // 当前值从 float64 转换为 uint64（实际表示bit）
	vbits ^= bits << newTrailing         // 有效数据左移空出 0，完整的值和读取的差值按位异或计算新的值
	*value = math.Float64frombits(vbits) // uint64（bit）转化为 float64，存入迭代器
	return nil
}

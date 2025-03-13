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
// It received minor modifications to suit Prometheus's needs.

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
	"io"
)

// bstream is a stream of bits.	// 数据的字节流
type bstream struct {
	stream []byte // The data stream.	字节流
	count  uint8  // How many right-most bits are available for writing in the current byte (the last byte of the stream).	当前字节 byte 中（流的最后一个字节）有多少最右侧的位 bit 可以写入（流的最后一个字节从最右侧起有多少位可用）
}

// Reset resets b around stream.	重置bstream
func (b *bstream) Reset(stream []byte) {
	b.stream = stream
	b.count = 0
}

// 获取字节流
func (b *bstream) bytes() []byte {
	return b.stream
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

// 向字节流中写入一位 bit	，入参 bit 是 bool 类型
func (b *bstream) writeBit(bit bit) {
	if b.count == 0 { // 若当前字节(流的最后一个字节) byte 中没有可用的位 bit，向流中添加一个空字节 0，可用位数设置为 8 （一个字节可用8位）
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1 // 流中最后一个字节的下标

	if bit { // 写入 bit 1	； bit 0 不用管，默认就是 0
		b.stream[i] |= 1 << (b.count - 1) // 位运算，获取一个当前字节最左侧的可用位置为 1，其余为 0 的字节 ，然后和原字节 按位异或，把原字节的该位置为 1，实现 bit 1 的写入
	}

	b.count-- // 该字节中的可用位数减一
}

// 向字节流中写入一个字节 byte
func (b *bstream) writeByte(byt byte) {
	if b.count == 0 { // 流中最后一个字节的可用位数为 0，添加一个空字节 0，可用位数设置为 8
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1 // 流中最后一个字节的下标

	// Complete the last byte with the leftmost b.count bits from byt.	先把 byt 最左的位填充进当前字节的剩余可用位中；如果是空字节，这里就全部写入了
	b.stream[i] |= byt >> (8 - b.count)

	b.stream = append(b.stream, 0) // 在流的末尾添加一个空字节，索引后移
	i++
	// Write the remainder, if any.	把 byt 右边的剩余位（count个）写入新的空字节；	因为共写入了 一字节8位，剩余可用位数量仍然是 count
	b.stream[i] = byt << b.count
}

// writeBits writes the nbits right-most bits of u to the stream
// in left-to-right order.
// 把 u 的最右侧的 nbits 个 bit 以从左到右的顺序写入字节流
func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= 64 - uint(nbits) // 左移，使 u 的左侧开始的最高位是待写入的 nbit 个 bit，右侧剩余位是 0
	for nbits >= 8 {       // 先以字节为单位写入
		byt := byte(u >> 56) // 获取 u 的左侧最高 8 bit
		b.writeByte(byt)     // 写入 1 byte
		u <<= 8              // u 左移 8 bit，去掉已写入的 bit
		nbits -= 8           // 剩余待写入位数减 8
	}

	for nbits > 0 { // 剩余不足 8 bit，按 bit 依次写入
		b.writeBit((u >> 63) == 1) // 该参数 bit 是自定义的，实际是 bool 类型，判断该 bit 是否为 1
		u <<= 1                    // 去掉写入的位
		nbits--
	}
}

// 字节流的 reader
type bstreamReader struct {
	stream       []byte // 流
	streamOffset int    // The offset from which read the next byte from the stream.	从流中读取下一字节的字节数偏移量

	buffer uint64 // The current buffer, filled from the stream, containing up to 8 bytes from which read bits.	由流填充的 buffer，容纳最多 8 bytes
	valid  uint8  // The number of right-most bits valid to read (from left) in the current 8 byte buffer.	当前的 8 byte buffer 中最右侧的可读位数（从左到右读这几个可读位）
	last   byte   // A copy of the last byte of the stream.		流的尾字节的复制
}

// 传入 bstream.stream，获取其 Reader
func newBReader(b []byte) bstreamReader {
	// The last byte of the stream can be updated later, so we take a copy.		流的尾字节可能会被更新，所以这里保存一个 copy: last
	var last byte
	if len(b) > 0 {
		last = b[len(b)-1] // 尾字节
	}
	return bstreamReader{ // 初始状态的 Reader，只有 流 和 尾字节
		stream: b,
		last:   last,
	}
}

// 从 流 中读取一个 bit
func (b *bstreamReader) readBit() (bit, error) {
	if b.valid == 0 { // buffer 中的可读位数为 0，加载下一个 buffer，若失败，返回 error EOF，说明流中没有可读位了
		if !b.loadNextBuffer(1) {
			return false, io.EOF
		}
	}

	return b.readBitFast()
}

// readBitFast is like readBit but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBit().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
// 叶函数：只包含基本指令和少量操作，不调用其他函数；	可以内联
// 从 Reader.buffer 中读取一个 bit，若 buffer 为空，返回 EOF
func (b *bstreamReader) readBitFast() (bit, error) {
	if b.valid == 0 { // 检查 buffer 中的可读字节数
		return false, io.EOF
	}

	b.valid--                             // 减一之后是实际可读的位置
	bitmask := uint64(1) << b.valid       // 获取最高的可读位的掩码，该位置为 1
	return (b.buffer & bitmask) != 0, nil // 获取 buffer 中的该位，返回对应的 bool；	掩码中除了该位都为 0，按位与的结果都是零，只有该位为 1，若 buffer 该位也为 1，按位与的结果就不为 0，若 buffer 该位为 0，按位与的结果就等于 0

	// 0b1011 & 0b1000 = 0b1000 != 0	0b1011 & 0b0100 = 0b0000 = 0
}

// readBits constructs a uint64 with the nbits right-most bits
// read from the stream, and any other bits 0.
// 从流中读取 nbits 个 bit，组成一个 uint64，最多读取 64 位
func (b *bstreamReader) readBits(nbits uint8) (uint64, error) {
	if b.valid == 0 { // 若 buffer 的可读位数为 0，加载下一个 buffer
		if !b.loadNextBuffer(nbits) {
			return 0, io.EOF
		}
	}

	if nbits <= b.valid { // 若 nbits 不多于可读位数，直接从 buffer 中 读取
		return b.readBitsFast(nbits)
	}

	// 数据跨越两个 buffer 时的读法
	// We have to read all remaining valid bits from the current buffer and a part from the next one.
	// buffer 最多有 8 byte，nbits 最多也是 64 bit，可能会跨越两个 buffer
	bitmask := (uint64(1) << b.valid) - 1 // 掩码，所有可读位都是 1	；valid 是可读位数，右移之后可读位的前一位置 1，所有可读位都是 0，减一后所有可读位都是 1，其他位是 0
	nbits -= b.valid
	v := (b.buffer & bitmask) << nbits // 按位与，获取 buffer 的原始数据（无效位经过计算都是0），存为结果的高位
	b.valid = 0

	if !b.loadNextBuffer(nbits) { // 加载下一个 buffer，读取剩余数据
		return 0, io.EOF
	}

	bitmask = (uint64(1) << nbits) - 1               // 待读取的所有位都置为 1
	v |= ((b.buffer >> (b.valid - nbits)) & bitmask) // 获取 buffer 的剩余数据	； 左移运算不会修改原始数据，buffer 实际上并没有变，只是 valid 数量减少，表示最右侧开始可读位数减少
	b.valid -= nbits

	return v, nil
}

// readBitsFast is like readBits but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBits().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
// 只有当待读取位数不多于可读位数时才能读取
// 保持内联
func (b *bstreamReader) readBitsFast(nbits uint8) (uint64, error) {
	if nbits > b.valid {
		return 0, io.EOF
	}

	bitmask := (uint64(1) << nbits) - 1 // 掩码，待读取位均为 1
	b.valid -= nbits                    // 读取后剩余可读位

	return (b.buffer >> b.valid) & bitmask, nil // 左移是为了把待读取位移到最右侧，用掩码获取原始数据
}

// ReadByte 从流中读取一个字节
func (b *bstreamReader) ReadByte() (byte, error) {
	v, err := b.readBits(8)
	if err != nil {
		return 0, err
	}
	return byte(v), nil
}

// loadNextBuffer loads the next bytes from the stream into the internal buffer.
// The input nbits is the minimum number of bits that must be read, but the implementation
// can read more (if possible) to improve performances.
// 从流中把接下来的 bytes 加载到 buffer 中，最多 8 byte
// nbits 是必须读取的最小位数，实现中可以读取更多
func (b *bstreamReader) loadNextBuffer(nbits uint8) bool {
	if b.streamOffset >= len(b.stream) { // 读取的偏移量越界
		return false
	}

	// Handle the case there are more then 8 bytes in the buffer (most common case)
	// in a optimized way. It's guaranteed that this branch will never read from the
	// very last byte of the stream (which suffers race conditions due to concurrent
	// writes).
	if b.streamOffset+8 < len(b.stream) { // 若读取 8 byte 之后还没到流的末尾，进入此分支读取
		b.buffer = binary.BigEndian.Uint64(b.stream[b.streamOffset:]) // 从当前的字节数偏移量处开始，Uint64()函数内部会自动截取8字节转换成 uint64，大端序
		b.streamOffset += 8                                           // 更新字节数偏移量
		b.valid = 64                                                  // 更新可读位数
		return true
	}

	// We're here if there are 8 or less bytes left in the stream.
	// The following code is slower but called less frequently.
	// 若流中剩余字节数 少于 8，由此处理读取
	nbytes := int((nbits / 8) + 1)             // 最少读取的字节数
	if b.streamOffset+nbytes > len(b.stream) { // 最少读取字节数越界，改为直到尾字节的长度
		nbytes = len(b.stream) - b.streamOffset
	}

	buffer := uint64(0)
	skip := 0
	if b.streamOffset+nbytes == len(b.stream) { // 恰好读完流
		// There can be concurrent writes happening on the very last byte
		// of the stream, so use the copy we took at initialization time.
		buffer |= uint64(b.last) // 把初始化时复制的尾字节存入 buffer 的最低位
		// Read up to the byte before
		skip = 1 // 跳过尾字节
	}

	for i := 0; i < nbytes-skip; i++ { // 逐字节读取
		buffer |= (uint64(b.stream[b.streamOffset+i]) << uint(8*(nbytes-i-1))) // 从 buffer 左侧到右侧一次存入
	}

	b.buffer = buffer
	b.streamOffset += nbytes
	b.valid = uint8(nbytes * 8) // 更新实际读取的字节信息

	return true
}

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

package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"unsafe"

	"github.com/dennwc/varint"
)

var (
	ErrInvalidSize     = errors.New("invalid size")
	ErrInvalidChecksum = errors.New("invalid checksum")
)

// Encbuf is a helper type to populate a byte slice with various types.	用于帮助各种类型的数据编码后填充字节流
type Encbuf struct {
	B []byte                      // 数据编码的字节流
	C [binary.MaxVarintLen64]byte // 在变长编码（varint）中，一个 64 位整数在编码时可能占用的最大字节数是 10
}

func (e *Encbuf) Reset()      { e.B = e.B[:0] }
func (e *Encbuf) Get() []byte { return e.B }
func (e *Encbuf) Len() int    { return len(e.B) }

func (e *Encbuf) PutString(s string) { e.B = append(e.B, s...) }
func (e *Encbuf) PutByte(c byte)     { e.B = append(e.B, c) }
func (e *Encbuf) PutBytes(b []byte)  { e.B = append(e.B, b...) }

func (e *Encbuf) PutBE32int(x int)      { e.PutBE32(uint32(x)) }
func (e *Encbuf) PutUvarint32(x uint32) { e.PutUvarint64(uint64(x)) }
func (e *Encbuf) PutBE64int64(x int64)  { e.PutBE64(uint64(x)) }
func (e *Encbuf) PutUvarint(x int)      { e.PutUvarint64(uint64(x)) }

func (e *Encbuf) PutBE32(x uint32) {
	binary.BigEndian.PutUint32(e.C[:], x) // uint32 转换成 字节数组
	e.B = append(e.B, e.C[:4]...)         // 转换后的 4 byte 存入字节流
}

func (e *Encbuf) PutBE64(x uint64) {
	binary.BigEndian.PutUint64(e.C[:], x)
	e.B = append(e.B, e.C[:8]...) // 只能转换 uint64 8 byte，不足 8 byte 的会报错
}

func (e *Encbuf) PutBEFloat64(x float64) {
	e.PutBE64(math.Float64bits(x)) // float64 可以直接转换成 byte
}

func (e *Encbuf) PutUvarint64(x uint64) {
	n := binary.PutUvarint(e.C[:], x) // uint64 ,可变长编码，不一定有 8 字节，不能用 PutBE64()
	e.B = append(e.B, e.C[:n]...)
}

func (e *Encbuf) PutVarint64(x int64) {
	n := binary.PutVarint(e.C[:], x) // int64, 可变长编码
	e.B = append(e.B, e.C[:n]...)
}

// PutUvarintStr writes a string to the buffer prefixed by its varint length (in bytes!).
// 字符串在写入字节流之前，需要先写入其长度
func (e *Encbuf) PutUvarintStr(s string) {
	b := *(*[]byte)(unsafe.Pointer(&s)) // 用指针转换成 byte
	e.PutUvarint(len(b))                // string 长度
	e.PutString(s)                      // string
}

// PutUvarintBytes writes a variable length byte buffer.
// 把字节数组写入字节流，先写入其长度
func (e *Encbuf) PutUvarintBytes(b []byte) {
	e.PutUvarint(len(b))
	e.PutBytes(b)
}

// PutHash appends a hash over the buffers current contents to the buffer.
// 将当前 Encbuf 内容的哈希值追加到 Encbuf 中
func (e *Encbuf) PutHash(h hash.Hash) {
	h.Reset()
	e.WriteToHash(h)
	e.PutHashSum(h)
}

// WriteToHash writes the current buffer contents to the given hash.
// 将 Encbuf 的当前内容写入给定的哈希对象 h
func (e *Encbuf) WriteToHash(h hash.Hash) {
	_, err := h.Write(e.B)
	if err != nil {
		panic(err) // The CRC32 implementation does not error
	}
}

// PutHashSum writes the Sum of the given hash to the buffer.
// 将给定哈希对象 h 的摘要（哈希值）追加到 Encbuf 的缓冲区中
func (e *Encbuf) PutHashSum(h hash.Hash) {
	e.B = h.Sum(e.B) // 计算哈希值，Sum 方法会返回一个新的切片，包含原始缓冲区内容和哈希值
}

// Decbuf provides safe methods to extract data from a byte slice. It does all
// necessary bounds checking and advancing of the byte slice.
// Several datums can be extracted without checking for errors. However, before using
// any datum, the err() method must be checked.
// 字节流解码缓冲区
type Decbuf struct {
	B []byte
	E error
}

// NewDecbufAt returns a new decoding buffer. It expects the first 4 bytes
// after offset to hold the big endian encoded content length, followed by the contents and the expected
// checksum.
// 获取一个新的 BigEndian 解码缓冲区，offset 后面的 4 byte 是大端序编码的数据内容长度，然后是数据和校验和，这里会验证校验和是否正确
func NewDecbufAt(bs ByteSlice, off int, castagnoliTable *crc32.Table) Decbuf {
	if bs.Len() < off+4 { // 读不到长度
		return Decbuf{E: ErrInvalidSize}
	}
	b := bs.Range(off, off+4)            // byte[off:off+4]
	l := int(binary.BigEndian.Uint32(b)) // 转换成数字，表示数据长度 byte length

	if bs.Len() < off+4+l+4 { // 待转换字节流的实际长度不够 off + len + data + checksum
		return Decbuf{E: ErrInvalidSize}
	}

	// Load bytes holding the contents plus a CRC32 checksum.
	b = bs.Range(off+4, off+4+l+4) // 获取字节流中的数据部分和校验和
	dec := Decbuf{B: b[:len(b)-4]} // 纯数据部分构造成 Decbuf

	if castagnoliTable != nil { // 若 crc32 算法的查找表不为空 （该函数的所有调用都传入了查找表，程序启动后查找表会自动初始化，所以这里必定进行校验）
		if exp := binary.BigEndian.Uint32(b[len(b)-4:]); dec.Crc32(castagnoliTable) != exp { // 解码字节流末尾的校验和，计算纯数据部分的校验和，两者应该相等
			return Decbuf{E: ErrInvalidChecksum}
		}
	}
	return dec
}

// NewDecbufUvarintAt returns a new decoding buffer. It expects the first bytes
// after offset to hold the uvarint-encoded buffers length, followed by the contents and the expected
// checksum.
// 获取一个新的 Varint 解码缓冲区，offset 后面的若干 bytes 是 uvarint 编码的 buffer length，然后是数据和校验和，这里会验证校验和是否正确
// 该函数只在字节流不长时使用，长度编码的字节数也不会太多
func NewDecbufUvarintAt(bs ByteSlice, off int, castagnoliTable *crc32.Table) Decbuf {
	// We never have to access this method at the far end of the byte slice. Thus just checking
	// against the MaxVarintLen32 is sufficient.
	if bs.Len() < off+binary.MaxVarintLen32 {
		return Decbuf{E: ErrInvalidSize}
	}
	b := bs.Range(off, off+binary.MaxVarintLen32) // 长度为 5

	l, n := varint.Uvarint(b) // 数据长度，实际读取的字节数
	if n <= 0 || n > binary.MaxVarintLen32 {
		return Decbuf{E: fmt.Errorf("invalid uvarint %d", n)}
	}

	if bs.Len() < off+n+int(l)+4 { // off + var len + data length + checksum
		return Decbuf{E: ErrInvalidSize}
	}

	// Load bytes holding the contents plus a CRC32 checksum.
	b = bs.Range(off+n, off+n+int(l)+4) // 字节流中的数据部分和校验和
	dec := Decbuf{B: b[:len(b)-4]}      // 纯数据部分构造成 Decbuf

	if dec.Crc32(castagnoliTable) != binary.BigEndian.Uint32(b[len(b)-4:]) { // 计算和读取的校验和是否相等
		return Decbuf{E: ErrInvalidChecksum}
	}
	return dec
}

// NewDecbufRaw returns a new decoding buffer of the given length.
// 返回字节流起始位置开始的指定长度的 Decbuf
func NewDecbufRaw(bs ByteSlice, length int) Decbuf {
	if bs.Len() < length {
		return Decbuf{E: ErrInvalidSize}
	}
	return Decbuf{B: bs.Range(0, length)}
}

func (d *Decbuf) Uvarint() int      { return int(d.Uvarint64()) }
func (d *Decbuf) Uvarint32() uint32 { return uint32(d.Uvarint64()) }
func (d *Decbuf) Be32int() int      { return int(d.Be32()) }
func (d *Decbuf) Be64int64() int64  { return int64(d.Be64()) }

// Crc32 returns a CRC32 checksum over the remaining bytes.
// 用解码缓冲区 Decbuf 中的剩余数据和预定义的查找表计算 crc32 校验和
func (d *Decbuf) Crc32(castagnoliTable *crc32.Table) uint32 {
	return crc32.Checksum(d.B, castagnoliTable)
}

// 跳过 buffer 中的 l 个字节
func (d *Decbuf) Skip(l int) {
	if len(d.B) < l {
		d.E = ErrInvalidSize
		return
	}
	d.B = d.B[l:]
}

// 调用 UvarintBytes，转换成 string，创建了一个 string 对象
func (d *Decbuf) UvarintStr() string {
	return string(d.UvarintBytes())
}

// UvarintBytes returns a pointer to internal data;
// the return value becomes invalid if the byte slice goes away.
// Compared to UvarintStr, this avoids allocations.
// 读取 uvarint64 编码的长度，返回 buffer 当前字节流起始到目标长度的 字节切片
// 该函数只是返回了 buffer 内部数据的切片的指针，没有创建新切片，返回值和原字节流的生命周期一致，和 UvarintStr 相比避免了内存分配
func (d *Decbuf) UvarintBytes() []byte {
	l := d.Uvarint64() // 读取一个长度
	if d.E != nil {
		return []byte{}
	}
	if len(d.B) < int(l) { // 长度越界
		d.E = ErrInvalidSize
		return []byte{}
	}
	s := d.B[:l]  // 从字节流起始读到长度 l
	d.B = d.B[l:] // 后移
	return s
}

// 读取并解码 buffer 中的一个 varint64，指针后移
func (d *Decbuf) Varint64() int64 {
	if d.E != nil {
		return 0
	}
	// Decode as unsigned first, since that's what the varint library implements.
	ux, n := varint.Uvarint(d.B) // 解码可变长 uint64
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	// Now decode "ZigZag encoding" https://developers.google.com/protocol-buffers/docs/encoding#signed_integers.
	x := int64(ux >> 1) // uint64 转换为 int64
	if ux&1 != 0 {
		x = ^x
	}
	d.B = d.B[n:]
	return x
}

// 读取并解码 buffer 中的一个 uvarint64，指针后移
func (d *Decbuf) Uvarint64() uint64 {
	if d.E != nil {
		return 0
	}
	x, n := varint.Uvarint(d.B) // 解码 长度，实际读取字节数
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	d.B = d.B[n:] // 跳过已解码的字节
	return x
}

// 读取并解码 buffer 的一个大端序 uint64，指针后移
func (d *Decbuf) Be64() uint64 {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 8 {
		d.E = ErrInvalidSize
		return 0
	}
	x := binary.BigEndian.Uint64(d.B) // 解码，大端序 定长 uint64
	d.B = d.B[8:]
	return x
}

// 调用 Be64, 读取的 uint64 转换为 float64
func (d *Decbuf) Be64Float64() float64 {
	return math.Float64frombits(d.Be64())
}

// 读取并解码 buffer 的一个大端序 uint32，指针后移
func (d *Decbuf) Be32() uint32 {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 4 {
		d.E = ErrInvalidSize
		return 0
	}
	x := binary.BigEndian.Uint32(d.B) // 解码 定长 uint32
	d.B = d.B[4:]
	return x
}

// 读取 buffer 的一个字节，指针后移
func (d *Decbuf) Byte() byte {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	x := d.B[0]
	d.B = d.B[1:]
	return x
}

// 去掉占位0
func (d *Decbuf) ConsumePadding() {
	if d.E != nil {
		return
	}
	for len(d.B) > 1 && d.B[0] == '\x00' {
		d.B = d.B[1:]
	}
	if len(d.B) < 1 {
		d.E = ErrInvalidSize
	}
}

func (d *Decbuf) Err() error  { return d.E }
func (d *Decbuf) Len() int    { return len(d.B) }
func (d *Decbuf) Get() []byte { return d.B }

// ByteSlice abstracts a byte slice.
// 表示数据字节流的一个接口
type ByteSlice interface {
	Len() int                    // 字节流长度
	Range(start, end int) []byte // 字节切片，byte[start, end)
}

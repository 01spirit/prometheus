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

package chunks

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

/*
	chunks 和 chunk 的结构详见 tsdb/docs/format/chunks.md
*/

// Segment header fields constants.
// chunks 文件（段文件）的头部，依次是 magic(4 byte)，version(1 byte)，padding(3 byte)，然后是分段存储的 chunk
const (
	// MagicChunks is 4 bytes at the head of a series file.		block chunk 的 header magic
	MagicChunks = 0x85BD40DD // magic
	// MagicChunksSize is the size in bytes of MagicChunks.
	MagicChunksSize          = 4
	chunksFormatV1           = 1
	ChunksFormatVersionSize  = 1
	segmentHeaderPaddingSize = 3
	// SegmentHeaderSize defines the total size of the header part.
	SegmentHeaderSize = MagicChunksSize + ChunksFormatVersionSize + segmentHeaderPaddingSize
)

// Chunk fields constants.	chunk 的结构是 len(uvarint 1-10 byte)，encoding(1 byte)，data(len bytes)，crc32(4 byte)
const (
	// MaxChunkLengthFieldSize defines the maximum size of the data length part.
	MaxChunkLengthFieldSize = binary.MaxVarintLen32 // len 字段（表示数据量）的最大长度是 5 byte
	// ChunkEncodingSize defines the size of the chunk encoding part.
	ChunkEncodingSize = 1
)

// ChunkRef is a generic reference for reading chunk data. In prometheus it
// is either a HeadChunkRef or BlockChunkRef, though other implementations
// may have their own reference types.
// 用来读取 chunk 数据的通用指针（reference），分为 HeadChunkRef 和 BlockChunkRef 两种
type ChunkRef uint64

// HeadSeriesRef refers to in-memory series.
// 指向 内存 series，最多 5 byte	可以当作 series ID
type HeadSeriesRef uint64

// HeadChunkRef packs a HeadSeriesRef and a ChunkID into a global 8 Byte ID.
// The HeadSeriesRef and ChunkID may not exceed 5 and 3 bytes respectively.
// 把 HeadSeriesRef(数据量不超过 5 byte，高位) 和 ChunkID(数据量不超过 3 byte，低位) 打包成一个全局的 8 byte ID
type HeadChunkRef uint64

// NewHeadChunkRef 把 HeadSeriesRef(最多 5 byte) 和 ChunkID(最多 3 byte) 打包成一个 HeadChukRef
func NewHeadChunkRef(hsr HeadSeriesRef, chunkID HeadChunkID) HeadChunkRef {
	if hsr > (1<<40)-1 { // series ID 超过 5 byte
		panic("series ID exceeds 5 bytes")
	}
	if chunkID > (1<<24)-1 { // chunk ID 超过 3 byte
		panic("chunk ID exceeds 3 bytes")
	}
	return HeadChunkRef(uint64(hsr<<24) | uint64(chunkID)) // 高五字节是 hsr ，低三字节是 chunkID
}

// Unpack 把 HeadChunkRef 中的 HeadSeriesRef (series ID，高五字节) 和 HeadChukID （低三字节）分离出来
func (p HeadChunkRef) Unpack() (HeadSeriesRef, HeadChunkID) {
	return HeadSeriesRef(p >> 24), HeadChunkID(p<<40) >> 40
}

// HeadChunkID refers to a specific chunk in a series (memSeries) in the Head.
// Each memSeries has its own monotonically increasing number to refer to its chunks.
// If the HeadChunkID value is...
//   - memSeries.firstChunkID+len(memSeries.mmappedChunks), it's the head chunk.
//   - less than the above, but >= memSeries.firstID, then it's
//     memSeries.mmappedChunks[i] where i = HeadChunkID - memSeries.firstID.
//
// If memSeries.headChunks is non-nil it points to a *memChunk that holds the current
// "open" (accepting appends) instance. *memChunk is a linked list and memChunk.next pointer
// might link to the older *memChunk instance.
// If there are multiple *memChunk instances linked to each other from memSeries.headChunks
// they will be m-mapped as soon as possible leaving only "open" *memChunk instance.
//
// Example:
// assume a memSeries.firstChunkID=7 and memSeries.mmappedChunks=[p5,p6,p7,p8,p9].
//
//	| HeadChunkID value | refers to ...                                                                          |
//	|-------------------|----------------------------------------------------------------------------------------|
//	|               0-6 | chunks that have been compacted to blocks, these won't return data for queries in Head |
//	|              7-11 | memSeries.mmappedChunks[i] where i is 0 to 4.                                          |
//	|                12 |                                                         *memChunk{next: nil}
//	|                13 |                                         *memChunk{next: ^}
//	|                14 | memSeries.headChunks -> *memChunk{next: ^}
//
// HeadChunkID 指向 Head 中的一个 series（内存 series）的一个 chunk。每个内存 series 有自己的单调递增的序列号指向它的 chunks。
type HeadChunkID uint64

// BlockChunkRef refers to a chunk within a persisted block.
// The upper 4 bytes are for the segment index and
// the lower 4 bytes are for the segment offset where the data starts for this chunk.
// 指向持久块中的一个 chunk，高 4 byte 是段（segment file）索引，低 4 byte是数据从段文件中起始的段偏移量
type BlockChunkRef uint64

// NewBlockChunkRef packs the file index and byte offset into a BlockChunkRef.
// 把段文件索引（高四字节）和段文件内chunk起始字节偏移量（低四字节）打包成 BlockChunkRef
func NewBlockChunkRef(fileIndex, fileOffset uint64) BlockChunkRef {
	return BlockChunkRef(fileIndex<<32 | fileOffset)
}

// Unpack 把 BlockChunkRef 中的 sgmFileIndex （段文件索引序列号） 和 fileOffset （chunk在段文件内的起始字节偏移量） 分离出来
func (b BlockChunkRef) Unpack() (int, int) {
	sgmIndex := int(b >> 32)
	chkStart := int((b << 32) >> 32)
	return sgmIndex, chkStart
}

// Meta holds information about one or more chunks.
// For examples of when chunks.Meta could refer to multiple chunks, see
// ChunkReader.ChunkOrIterable().
// 一或多个 chunk 的元信息
type Meta struct {
	// Ref and Chunk hold either a reference that can be used to retrieve
	// chunk data or the data itself.
	// If Chunk is nil, call ChunkReader.ChunkOrIterable(Meta.Ref) to get the
	// chunk and assign it to the Chunk field. If an iterable is returned from
	// that method, then it may not be possible to set Chunk as the iterable
	// might form several chunks.
	Ref   ChunkRef       // 获取 chunk 数据的指针
	Chunk chunkenc.Chunk // chunk 的数据

	// Time range the data covers.
	// When MaxTime == math.MaxInt64 the chunk is still open and being appended to.
	MinTime, MaxTime int64 // chunk 数据的实际时间范围，MaxTime == math.MaxInt64 表示可以向 chunk 中添加数据
}

// ChunkFromSamples requires all samples to have the same type.
// 用 Samples 创建并填充一个新的 Chunk，返回其 Meta，Meta.Ref 为空
func ChunkFromSamples(s []Sample) (Meta, error) {
	return ChunkFromSamplesGeneric(SampleSlice(s))
}

// ChunkFromSamplesGeneric requires all samples to have the same type.
// 用 Samples 创建并填充一个新的 Chunk，返回其 Meta，Meta.Ref 为空
func ChunkFromSamplesGeneric(s Samples) (Meta, error) {
	emptyChunk := Meta{Chunk: chunkenc.NewXORChunk()} // 创建一个 XORChunk，只用于异常返回
	mint, maxt := int64(0), int64(0)

	if s.Len() > 0 { // 获取数据切片的起止时间
		mint, maxt = s.Get(0).T(), s.Get(s.Len()-1).T()
	}

	if s.Len() == 0 {
		return emptyChunk, nil
	}

	sampleType := s.Get(0).Type()                                // sample 的类型，用 float64
	c, err := chunkenc.NewEmptyChunk(sampleType.ChunkEncoding()) // 用编码方式创建一个新的 chunk，向其中添加数据
	if err != nil {
		return Meta{}, err
	}

	ca, _ := c.Appender() // chunk Appender
	var newChunk chunkenc.Chunk

	for i := 0; i < s.Len(); i++ {
		switch sampleType {
		case chunkenc.ValFloat:
			ca.Append(s.Get(i).T(), s.Get(i).F()) // 向 chunk c 中添加数据
		case chunkenc.ValHistogram:
			newChunk, _, ca, err = ca.AppendHistogram(nil, s.Get(i).T(), s.Get(i).H(), false)
			if err != nil {
				return emptyChunk, err
			}
			if newChunk != nil {
				return emptyChunk, fmt.Errorf("did not expect to start a second chunk")
			}
		case chunkenc.ValFloatHistogram:
			newChunk, _, ca, err = ca.AppendFloatHistogram(nil, s.Get(i).T(), s.Get(i).FH(), false)
			if err != nil {
				return emptyChunk, err
			}
			if newChunk != nil {
				return emptyChunk, fmt.Errorf("did not expect to start a second chunk")
			}
		default:
			panic(fmt.Sprintf("unknown sample type %s", sampleType.String()))
		}
	}
	return Meta{ // 构建并返回 chunk Meta
		MinTime: mint,
		MaxTime: maxt,
		Chunk:   c,
	}, nil
}

// ChunkMetasToSamples converts a slice of chunk meta data to a slice of samples.
// Used in tests to compare the content of chunks.
// 取出 chunks 中的所有 Sample，只用于测试比较 chunks 的内容
func ChunkMetasToSamples(chunks []Meta) (result []Sample) {
	if len(chunks) == 0 {
		return
	}

	for _, chunk := range chunks {
		it := chunk.Chunk.Iterator(nil)                               // 为每个 chunk 创建迭代器
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() { // 迭代读取数据， vt 是数据类型
			switch vt {
			case chunkenc.ValFloat:
				t, v := it.At()
				result = append(result, sample{t: t, f: v}) // 存入结果切片
			case chunkenc.ValHistogram:
				t, h := it.AtHistogram(nil)
				result = append(result, sample{t: t, h: h})
			case chunkenc.ValFloatHistogram:
				t, fh := it.AtFloatHistogram(nil)
				result = append(result, sample{t: t, fh: fh})
			default:
				panic("unexpected value type")
			}
		}
	}
	return
}

// Iterator iterates over the chunks of a single time series.
// 对单个时间序列的 chunks 的迭代器接口
type Iterator interface {
	// At returns the current meta.
	// It depends on the implementation whether the chunk is populated or not.
	// 返回当前迭代器的 Meta 元信息
	At() Meta
	// Next advances the iterator by one.
	// 前移迭代器
	Next() bool
	// Err returns optional error if Next is false.
	Err() error
}

// writeHash writes the chunk encoding and raw data into the provided hash.
// 向 hash 对象中写入 Encoding（1 byte） 和 chunk 的数据 （Bytes）
func (cm *Meta) writeHash(h hash.Hash, buf []byte) error {
	buf = append(buf[:0], byte(cm.Chunk.Encoding())) // 在数据前添加 1 byte 编码方式（ EncXOR ） uint8
	if _, err := h.Write(buf[:1]); err != nil {      // 把编码方式写入 hash 对象
		return err
	}
	if _, err := h.Write(cm.Chunk.Bytes()); err != nil { // 把 chunk 的数据写入 hash 对象
		return err
	}
	return nil
}

// OverlapsClosedInterval Returns true if the chunk overlaps [mint, maxt].
// 判断 chunk 数据和给定时间范围是否有重叠，重叠返回 true
func (cm *Meta) OverlapsClosedInterval(mint, maxt int64) bool {
	// The chunk itself is a closed interval [cm.MinTime, cm.MaxTime].
	return cm.MinTime <= maxt && mint <= cm.MaxTime // chunk 的最小时间大于给定的最大时间，或 chunk 的最大时间小于给定的最小时间时，返回  false
}

var errInvalidSize = fmt.Errorf("invalid size")

// 用于生成 CRC32 校验码的查找表，简化计算
var castagnoliTable *crc32.Table

// 程序启动时自动调用，初始化查找表
func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
// 返回一个新的 hash.Hash32 类型的对象,使用了一个预配置的多项式来初始化CRC32哈希。
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// Check if the CRC of data matches that stored in sum, computed when the chunk was stored.
// 检查校验和是否一致
func checkCRC32(data, sum []byte) error {
	got := crc32.Checksum(data, castagnoliTable)
	// This combination of shifts is the inverse of digest.Sum() in go/src/hash/crc32.
	want := uint32(sum[0])<<24 + uint32(sum[1])<<16 + uint32(sum[2])<<8 + uint32(sum[3])
	if got != want {
		return fmt.Errorf("checksum mismatch expected:%x, actual:%x", want, got)
	}
	return nil
}

// Writer implements the ChunkWriter interface for the standard
// serialization format.
// 为标准的序列化格式实现了 ChunkWriter 接口
type Writer struct {
	dirFile *os.File                    // chunks目录句柄
	files   []*os.File                  // 目录下的所有段文件
	wbuf    *bufio.Writer               // Writer buffer	初始化为 8 MB，可以自动把数据刷到底层的 io.Writer
	n       int64                       // 当前段文件中的字节数
	crc32   hash.Hash                   // 校验码
	buf     [binary.MaxVarintLen32]byte // 处理读写的 buffer ，5 byte

	segmentSize int64 // 段的大小
}

const (
	// DefaultChunkSegmentSize is the default chunks segment size.
	// chunks 中的段文件的默认大小是 512 MB
	DefaultChunkSegmentSize = 512 * 1024 * 1024
)

// NewWriterWithSegSize returns a new writer against the given directory
// and allows setting a custom size for the segments.
// 用给定的目录路径和段大小创建目录，获取其 Writer
func NewWriterWithSegSize(dir string, segmentSize int64) (*Writer, error) {
	return newWriter(dir, segmentSize)
}

// NewWriter returns a new writer against the given directory
// using the default segment size.
// 用给定的目录路径和默认的段大小创建目录，获取其 Writer
func NewWriter(dir string) (*Writer, error) {
	return newWriter(dir, DefaultChunkSegmentSize)
}

// 用给定的目录路径和段大小创建目录，获取该目录的一个新的 Writer
func newWriter(dir string, segmentSize int64) (*Writer, error) {
	if segmentSize <= 0 { // 默认 512MB
		segmentSize = DefaultChunkSegmentSize
	}

	if err := os.MkdirAll(dir, 0o777); err != nil { //创建目录，设置权限为所有用户可以读写和执行
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir) // 打开创建的目录，返回文件句柄
	if err != nil {
		return nil, err
	}
	return &Writer{ // 构建 Writer
		dirFile:     dirFile,
		n:           0,
		crc32:       newCRC32(),
		segmentSize: segmentSize,
	}, nil
}

// 获取目录下最后一个段文件的句柄
func (w *Writer) tail() *os.File {
	if len(w.files) == 0 {
		return nil
	}
	return w.files[len(w.files)-1]
}

// finalizeTail writes all pending data to the current tail file,
// truncates its size, and closes it.
// 把当前目录下的最后一个文件数据落盘，清理文件的空余空间
func (w *Writer) finalizeTail() error {
	tf := w.tail() // 获取末尾文件
	if tf == nil {
		return nil
	}

	// 把 writer buffer 中的所有数据刷新到底层的 io.Writer
	if err := w.wbuf.Flush(); err != nil {
		return err
	}
	// 把文件内容同步到底层存储设备（磁盘）
	if err := tf.Sync(); err != nil {
		return err
	}
	// As the file was pre-allocated, we truncate any superfluous zero bytes.
	off, err := tf.Seek(0, io.SeekCurrent) // 获取文件当前位置的偏移量
	if err != nil {
		return err
	}
	// 截断文件，移除文件中当前位置之后的所有数据，因为文件可能预先分配了空间，需要截断多余的零字节
	if err := tf.Truncate(off); err != nil {
		return err
	}

	return tf.Close() // 关闭文件，释放资源
}

// 切换段文件：落盘当前目录下的最后一个段文件，新建一个段文件，重置 writer buffer（可自动扩容）
func (w *Writer) cut() error {
	// Sync current tail to disk and close.
	// 落盘当前目录下的末尾文件
	if err := w.finalizeTail(); err != nil {
		return err
	}
	// 创建新的段文件，初始化文件 header
	n, f, _, err := cutSegmentFile(w.dirFile, MagicChunks, chunksFormatV1, w.segmentSize)
	if err != nil {
		return err
	}
	w.n = int64(n) // 当前段文件中的字节数

	w.files = append(w.files, f) // 新的段文件添加到 Writer 的末尾
	// 获取 Writer 中的空 buffer
	if w.wbuf != nil {
		w.wbuf.Reset(f) // 重置 write buffer
	} else {
		w.wbuf = bufio.NewWriterSize(f, 8*1024*1024) // 创建 8MB write buffer
	}

	return nil
}

// 切换段文件：在给定目录下，落盘之前的段文件，创建一个新的段文件，写入段文件的 8 byte header
func cutSegmentFile(dirFile *os.File, magicNumber uint32, chunksFormat byte, allocSize int64) (headerSize int, newFile *os.File, seq int, returnErr error) {
	p, seq, err := nextSequenceFile(dirFile.Name()) // 获取新的段文件名（六位数字格式）及其编号
	if err != nil {
		return 0, nil, 0, fmt.Errorf("next sequence file: %w", err)
	}
	ptmp := p + ".tmp"                                          // ~/chunks_dir/000001.tmp	，防止被其他进程意外访问或锁定：Reader 会跳过文件名不是只有数字的 tmp 文件
	f, err := os.OpenFile(ptmp, os.O_WRONLY|os.O_CREATE, 0o666) // 打开或创建段文件，只写模式，用户有读写权限
	if err != nil {
		return 0, nil, 0, fmt.Errorf("open temp file: %w", err)
	}
	defer func() { // 函数退出前调用，关闭文件，若出错，删除文件
		if returnErr != nil {
			errs := tsdb_errors.NewMulti(returnErr)
			if f != nil {
				errs.Add(f.Close())
			}
			// Calling RemoveAll on a non-existent file does not return error.
			errs.Add(os.RemoveAll(ptmp))
			returnErr = errs.Err()
		}
	}()
	// 段文件的大小，为文件分配空间
	if allocSize > 0 {
		if err = fileutil.Preallocate(f, allocSize, true); err != nil {
			return 0, nil, 0, fmt.Errorf("preallocate: %w", err)
		}
	}
	// 之前的数据落盘
	if err = dirFile.Sync(); err != nil {
		return 0, nil, 0, fmt.Errorf("sync directory: %w", err)
	}

	// Write header metadata for new file.
	// 编码新文件的 header 数据 magic 和 encoding
	metab := make([]byte, SegmentHeaderSize)                         // 段文件的头部存储元数据，8 字节
	binary.BigEndian.PutUint32(metab[:MagicChunksSize], magicNumber) // 编码 magic 4 byte
	metab[4] = chunksFormat                                          // 写入 version，1 byte

	n, err := f.Write(metab) // 8 byte header 元数据写入段文件，返回写入的字节数
	if err != nil {
		return 0, nil, 0, fmt.Errorf("write header: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, nil, 0, fmt.Errorf("close temp file: %w", err)
	}
	f = nil

	if err := fileutil.Rename(ptmp, p); err != nil { // 文件重命名，去掉 .tmp，表明这是一个正式的段文件，可以进行操作
		return 0, nil, 0, fmt.Errorf("replace file: %w", err)
	}

	f, err = os.OpenFile(p, os.O_WRONLY, 0o666) // 只写模式重新打开文件
	if err != nil {
		return 0, nil, 0, fmt.Errorf("open final file: %w", err)
	}
	// Skip header for further writes.	跳过 header，用于检查错误
	if _, err := f.Seek(int64(n), 0); err != nil {
		return 0, nil, 0, fmt.Errorf("seek in final file: %w", err)
	}
	return n, f, seq, nil
}

// 把字节流写入 writer buffer
func (w *Writer) write(b []byte) error {
	n, err := w.wbuf.Write(b) // 把字节流写入 writer buffer
	w.n += int64(n)           // 字节数量
	return err
}

/*
把 chunks 写入磁盘时，WriteChunks() 函数会把 chunks 划分到多个 segment file 中（内部通过 batches 分组实现），
确保不会超出 sgm file 容量限制；然后调用 writeChunks()  函数，把一组 chunks 的数据都写入 Writer.wbuf 中（可以自动把数据刷到底层的 io.Writer）；
如果当前不是最后一个 sgm file，调用 cut() 方法，落盘刚才填满的段文件（cut() 调用了 finalizeTail() 方法进行落盘和文件裁剪），
创建一个新的段文件用于写入接下来的 chunks，新段文件的 8 byte header 在这里由 cutSegmentFile() 函数写入；
如果是最后一个段文件，暂时不落盘，因为未必已经写满，等到下一次 WriteChunks()，会判断第一个 chunk 能否写入这个段文件；
如果第一个 chunk 不能写入，batches[0] 为空，writeChunks()函数会直接返回，由 WriteChunks() 函数直接落盘最后一个段文件并创建新的。
*/

// WriteChunks writes as many chunks as possible to the current segment,
// cuts a new segment when the current segment is full and
// writes the rest of the chunks in the new segment.
// 把 chunks 分组完整写入多个段文件，每个段文件不超过其设置的大小：向当前段文件中尽可能多的写入 chunks，当前段文件满了之后切换到新的段文件，向新的里面写入剩余的 chunks
func (w *Writer) WriteChunks(chks ...Meta) error {
	var (
		batchSize  = int64(0)          // batch 中的所有 chunk 的字节数量
		batchStart = 0                 // batch 中的起始 chunk 的 序号
		batches    = make([][]Meta, 1) // 每个 batch 中存的是写入同一个 segment 的 chunks
		batchID    = 0                 // batch 序号
		firstBatch = true              // 是第一个 batch
	)

	// 每次切换 batch，就表示需要新的 segment
	// 先把 chunks 划分成 batches，再把 batches 分别存入不同 segment
	for i, chk := range chks {
		// Each chunk contains: data length + encoding + the data itself + crc32
		// 一个 chunk 的全部数据的字节数
		chkSize := int64(MaxChunkLengthFieldSize) // The data length is a variable length field so use the maximum possible value. 数据长度，最多 5 byte
		chkSize += ChunkEncodingSize              // The chunk encoding.	编码长度
		chkSize += int64(len(chk.Chunk.Bytes()))  // The data itself.	数据
		chkSize += crc32.Size                     // The 4 bytes of crc32.	校验码
		batchSize += chkSize                      // 批量写入的字节数

		// Cut a new batch when it is not the first chunk(to avoid empty segments) and
		// the batch is too large to fit in the current segment.
		// 切换新的 batch：不是第一个 chunk && batch 过大，不能写入当前段文件
		cutNewBatch := (i != 0) && (batchSize+SegmentHeaderSize > w.segmentSize)

		// If the segment already has some data then
		// the first batch size calculation should account for that.
		// 若要写入的第一个 segment 中已有数据（n bytes），判断首个 batch 是否需要切换
		if firstBatch && w.n > SegmentHeaderSize { // 已有数据
			cutNewBatch = batchSize+w.n > w.segmentSize // 是否需要切换
			if cutNewBatch {
				firstBatch = false
			}
		}

		if cutNewBatch { // 切换新的 batch
			batchStart = i // 新 branch 的第一个 chunk 的序号，因为 i 已经装不下了，所以上一个 batch 到 i-1 为止，新的从 i 开始
			batches = append(batches, []Meta{})
			batchID++
			batchSize = chkSize // 第 i 个 chunk（新 batch 中的第一个 chunk 的数据量）
		}
		batches[batchID] = chks[batchStart : i+1] // 向 batches 中存入当前的 chunk
	}

	// Create a new segment when one doesn't already exist.
	// 当前目录下不存在段文件，创建并切换到新的
	if w.n == 0 {
		if err := w.cut(); err != nil {
			return err
		}
	}

	// 把 batches 里的 chunks 存入不同的 segment
	for i, chks := range batches {
		// 直接向 segment file 中写入 chunks，因为已经把 能写入同一个 segment file 的 chunks 放入同一个 batches 了
		if err := w.writeChunks(chks); err != nil {
			return err
		}
		// Cut a new segment only when there are more chunks to write.
		// Avoid creating a new empty segment at the end of the write.
		if i < len(batches)-1 { // 切换段文件，当前不是最后一个 batches ，还需要新的 sgm file
			if err := w.cut(); err != nil {
				return err
			}
		}
		// 最后一个 sgm file 暂时不会落盘，因为未必写满了
	}
	return nil
}

// writeChunks writes the chunks into the current segment irrespective
// of the configured segment size limit. A segment should have been already
// started before calling this.
// 无视配置的段文件大小限制，向当前段文件（这里实际只写入了 8 MB Writer buffer）中写入 chunks 的数据，在调用该函数之前一个段文件应该已经初始化好了
func (w *Writer) writeChunks(chks []Meta) error {
	// 这个段文件对应的 batches 中没有 chunk，可能是第一个段文件中已经放不下第一个 chunk 了，这里不再处理，由上级函数创建新的 sgm file
	if len(chks) == 0 {
		return nil
	}

	seq := uint64(w.seq()) // 待写入的段文件的序号（索引）
	for i := range chks {
		chk := &chks[i]

		chk.Ref = ChunkRef(NewBlockChunkRef(seq, uint64(w.n))) // 构造当前 chunk 的指针：高四字节段文件序号，低四字节段文件内字节偏移量

		n := binary.PutUvarint(w.buf[:], uint64(len(chk.Chunk.Bytes()))) // 向 buf 中写入 chunk 数据的长度

		// 详见 chunk.md 的 chunk 结构
		if err := w.write(w.buf[:n]); err != nil { // buf 数据写入 write buffer
			return err
		}
		w.buf[0] = byte(chk.Chunk.Encoding()) // 编码方式
		if err := w.write(w.buf[:1]); err != nil {
			return err
		}
		// 数据写入 writer buffer
		if err := w.write(chk.Chunk.Bytes()); err != nil {
			return err
		}

		w.crc32.Reset() // 重置该 chunk 的校验码
		if err := chk.writeHash(w.crc32, w.buf[:]); err != nil {
			return err
		}
		if err := w.write(w.crc32.Sum(w.buf[:0])); err != nil { // 向 chunk 写入校验码
			return err
		}
	}
	return nil
}

// 目录下最后一个段文件的序号
func (w *Writer) seq() int {
	return len(w.files) - 1
}

// Close 关闭整个 chunks 目录
func (w *Writer) Close() error {
	if err := w.finalizeTail(); err != nil { // 文件数据落盘
		return err
	}

	// close dir file (if not windows platform will fail on rename)
	return w.dirFile.Close() // 关闭目录
}

// ByteSlice abstracts a byte slice.
// 表示数据字节流的一个接口，在 encoding/encodings.go 中也有相同的定义，这里重复定义之后，就不需要在使用时引用 encoding 包了
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

// 实现 ByteSlice 接口
type realByteSlice []byte

// 字节流长度
func (b realByteSlice) Len() int {
	return len(b)
}

// 字节流切片，[ s, e )
func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

// Reader implements a ChunkReader for a serialized byte stream
// of series data.
// 为时序数据的序列化字节流实现了一个 ChunkReader 接口
type Reader struct {
	// The underlying bytes holding the encoded series data.
	// Each slice holds the data for a different segment.
	bs   []ByteSlice   // 时序数据的字节流，二维数组，表示每个 segment file 中的字节数组
	cs   []io.Closer   // Closers for resources behind the byte slices.	io.Closer 是一个接口，定义了一个 Close() 方法，任何实现了该方法的类型都可以当作 io.Closer 类型
	size int64         // The total size of bytes in the reader.	reader 中的有效数据的总字节数
	pool chunkenc.Pool // chunk pool
}

// 创建 Reader 结构，读取并验证 segment file header 信息，统计有效数据总长度
func newReader(bs []ByteSlice, cs []io.Closer, pool chunkenc.Pool) (*Reader, error) {
	cr := Reader{pool: pool, bs: bs, cs: cs} // 构建 Reader 结构
	for i, b := range cr.bs {                // 遍历所有 字节数组
		if b.Len() < SegmentHeaderSize {
			return nil, fmt.Errorf("invalid segment header in segment %d: %w", i, errInvalidSize)
		}
		// Verify magic number.
		if m := binary.BigEndian.Uint32(b.Range(0, MagicChunksSize)); m != MagicChunks { // 验证 magic number
			return nil, fmt.Errorf("invalid magic number %x", m)
		}

		// Verify chunk format version.
		if v := int(b.Range(MagicChunksSize, MagicChunksSize+ChunksFormatVersionSize)[0]); v != chunksFormatV1 { // 验证 version
			return nil, fmt.Errorf("invalid chunk format version %d", v)
		}
		cr.size += int64(b.Len()) // 有效数据的总长度
	}
	return &cr, nil
}

// NewDirReader returns a new Reader against sequentially numbered files in the
// given directory.
// 创建给定目录下的按序列号命名的段文件的 Reader
func NewDirReader(dir string, pool chunkenc.Pool) (*Reader, error) {
	files, err := sequenceFiles(dir) // 获取目录下所有存储数据的段文件的文件名
	if err != nil {
		return nil, err
	}
	if pool == nil {
		pool = chunkenc.NewPool() // 创建存储 chunk 的 pool
	}

	var (
		bs []ByteSlice
		cs []io.Closer
	)
	for _, fn := range files {
		f, err := fileutil.OpenMmapFile(fn) // 打开段文件，获取其句柄和数据字节流
		if err != nil {
			return nil, tsdb_errors.NewMulti(
				fmt.Errorf("mmap files: %w", err),
				tsdb_errors.CloseAll(cs),
			).Err()
		}
		cs = append(cs, f)                        // MmapFile(f) 实现了 Close() 方法，可以作为 io.Closer 类型
		bs = append(bs, realByteSlice(f.Bytes())) // 保存每个 sgm file 的字节流
	}

	reader, err := newReader(bs, cs, pool) // 构建 Reader 结构，验证和统计 header 信息
	if err != nil {
		return nil, tsdb_errors.NewMulti(
			err,
			tsdb_errors.CloseAll(cs),
		).Err()
	}
	return reader, nil
}

func (s *Reader) Close() error {
	return tsdb_errors.CloseAll(s.cs)
}

// Size returns the size of the chunks.
// reader 中的总字节数
func (s *Reader) Size() int64 {
	return s.size
}

// ChunkOrIterable returns a chunk from a given reference.
// 获取给定 reference （ BlockChunkRef 段索引+段内偏移量） 对应的 block chunk
func (s *Reader) ChunkOrIterable(meta Meta) (chunkenc.Chunk, chunkenc.Iterable, error) {
	sgmIndex, chkStart := BlockChunkRef(meta.Ref).Unpack() // 分离出 block chunk 的段索引和起始偏移量

	// 段索引超过 Reader 读取的段文件序列号(从零递增)
	if sgmIndex >= len(s.bs) {
		return nil, nil, fmt.Errorf("segment index %d out of range", sgmIndex)
	}

	sgmBytes := s.bs[sgmIndex] // 该段文件对应的字节流数据

	// chunk 起始地址 + len 超过段文件的实际数据长度
	if chkStart+MaxChunkLengthFieldSize > sgmBytes.Len() {
		return nil, nil, fmt.Errorf("segment doesn't include enough bytes to read the chunk size data field - required:%v, available:%v", chkStart+MaxChunkLengthFieldSize, sgmBytes.Len())
	}
	// With the minimum chunk length this should never cause us reading
	// over the end of the slice.
	c := sgmBytes.Range(chkStart, chkStart+MaxChunkLengthFieldSize)
	chkDataLen, n := binary.Uvarint(c) // 读取并解码 chunk 内数据长度，返回实际读取字节数
	if n <= 0 {
		return nil, nil, fmt.Errorf("reading chunk length failed with %d", n)
	}

	//┌───────────────┬───────────────────┬──────────────┬────────────────┐
	//│ len <uvarint> │ encoding <1 byte> │ data <bytes> │ CRC32 <4 byte> │
	//└───────────────┴───────────────────┴──────────────┴────────────────┘
	chkEncStart := chkStart + n                                              // encoding 字段的起始
	chkEnd := chkEncStart + ChunkEncodingSize + int(chkDataLen) + crc32.Size //  chunk 的末尾（crc 后面）
	chkDataStart := chkEncStart + ChunkEncodingSize                          // data 字段的起始
	chkDataEnd := chkEnd - crc32.Size                                        // data 字段的末尾

	if chkEnd > sgmBytes.Len() { // 检查越界
		return nil, nil, fmt.Errorf("segment doesn't include enough bytes to read the chunk - required:%v, available:%v", chkEnd, sgmBytes.Len())
	}

	sum := sgmBytes.Range(chkDataEnd, chkEnd)                                        // 读取存储的 CRC 校验码
	if err := checkCRC32(sgmBytes.Range(chkEncStart, chkDataEnd), sum); err != nil { // 计算读取的 encoding 和 data 的 CRC 校验码，验证是否无误
		return nil, nil, err
	}

	chkData := sgmBytes.Range(chkDataStart, chkDataEnd)                     // 数据部分
	chkEnc := sgmBytes.Range(chkEncStart, chkEncStart+ChunkEncodingSize)[0] // 编码方式	XORChunk 是 1
	chk, err := s.pool.Get(chunkenc.Encoding(chkEnc), chkData)              // pool.Get() 根据编码方式获取相应的 chunk 的地址，用指定字节流重置 chunk
	return chk, nil, err
}

// 获取目录下新的段文件的文件名（六位数字格式）和编号（从 0 递增）
func nextSequenceFile(dir string) (string, int, error) {
	files, err := os.ReadDir(dir) // 获取目录下的所有条目 entry，按文件名排序
	if err != nil {
		return "", 0, err
	}

	i := uint64(0) // 文件编号（数字格式的文件名）
	for _, f := range files {
		j, err := strconv.ParseUint(f.Name(), 10, 64) // 文件名转换为 uint64
		if err != nil {                               // 若文件名不是数字格式，跳过
			continue
		}
		// It is not necessary that we find the files in number order,
		// for example with '1000000' and '200000', '1000000' would come first.
		// Though this is a very very rare case, we check anyway for the max id.
		if j > i {
			i = j // 同步到最后的文件编号
		}
	}
	return segmentFile(dir, int(i+1)), int(i + 1), nil // 按照编号构建新的文件名，并返回编号
}

// 构建段文件路径，传入上级目录（chunks dir）和索引（文件编号），段文件名格式化为六位数字:~/chunks_dir/000001
func segmentFile(baseDir string, index int) string {
	return filepath.Join(baseDir, fmt.Sprintf("%0.6d", index))
}

// 获取目录下所有存储数据的段文件（按序列号命名）的文件名
func sequenceFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir) // 读取目录下的所有文件名
	if err != nil {
		return nil, err
	}
	var res []string
	for _, fi := range files {
		if _, err := strconv.ParseUint(fi.Name(), 10, 64); err != nil { // 文件不是按照序列号命名的，跳过
			continue
		}
		res = append(res, filepath.Join(dir, fi.Name())) // 按照序列号命名的存储数据的段文件
	}
	return res, nil
}

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

package chunks

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	"github.com/dennwc/varint"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

// Head chunk file header fields constants.
const (
	// MagicHeadChunks is 4 bytes at the beginning of a head chunk file.	 head chunk 的 header magic
	MagicHeadChunks = 0x0130BC91

	headChunksFormatV1 = 1
)

// ErrChunkDiskMapperClosed returned by any method indicates
// that the ChunkDiskMapper was closed.
var ErrChunkDiskMapperClosed = errors.New("ChunkDiskMapper closed")

const (
	// MintMaxtSize is the size of the mint/maxt for head chunk file and chunks.	时间戳长度
	MintMaxtSize = 8
	// SeriesRefSize is the size of series reference on disk.	磁盘上的 Series Reference
	SeriesRefSize = 8
	// HeadChunkFileHeaderSize is the total size of the header for a head chunk file.	chunks_head 文件的 header 也是 8 byte，magic 和 sgm file 的不同
	HeadChunkFileHeaderSize = SegmentHeaderSize
	// MaxHeadChunkFileSize is the max size of a head chunk file.	chunks_head 文件最大 128 MB
	MaxHeadChunkFileSize = 128 * 1024 * 1024 // 128 MiB.
	// CRCSize is the size of crc32 sum on disk.
	CRCSize = 4
	// MaxHeadChunkMetaSize is the max size of an mmapped chunks minus the chunks data.		head chunk 除 数据之外其他所有元数据的大小
	// Max because the uvarint size can be smaller.
	MaxHeadChunkMetaSize = SeriesRefSize + 2*MintMaxtSize + ChunkEncodingSize + MaxChunkLengthFieldSize + CRCSize
	// MinWriteBufferSize is the minimum write buffer size allowed.		write buffer 最小是 64 KB
	MinWriteBufferSize = 64 * 1024 // 64KB.
	// MaxWriteBufferSize is the maximum write buffer size allowed.		write buffer 最大是 8 MB
	MaxWriteBufferSize = 8 * 1024 * 1024 // 8 MiB.
	// DefaultWriteBufferSize is the default write buffer size.			write buffer 默认是 4 MB
	DefaultWriteBufferSize = 4 * 1024 * 1024 // 4 MiB.
	// DefaultWriteQueueSize is the default size of the in-memory queue used before flushing chunks to the disk.	在把 chunks 刷到磁盘上之前，内存队列的默认长度；0 表示禁止该功能
	// A value of 0 completely disables this feature.
	DefaultWriteQueueSize = 0
)

// ChunkDiskMapperRef represents the location of a head chunk on disk.
// The upper 4 bytes hold the index of the head chunk file and
// the lower 4 bytes hold the byte offset in the head chunk file where the chunk starts.
// 表示一个 head chunk 在磁盘上的位置：高四字节是 head chunk file 的索引序号，低四字节是 chunk 起始位置在 head chunk file 中的字节偏移量
type ChunkDiskMapperRef uint64

// head chunk file 结构 （chunks_head 目录）
//┌──────────────────────────────┐
//│  magic(0x0130BC91) <4 byte>  │
//├──────────────────────────────┤
//│    version(1) <1 byte>       │
//├──────────────────────────────┤
//│    padding(0) <3 byte>       │
//├──────────────────────────────┤
//│ ┌──────────────────────────┐ │
//│ │         Chunk 1          │ │
//│ ├──────────────────────────┤ │
//│ │          ...             │ │
//│ ├──────────────────────────┤ │
//│ │         Chunk N          │ │
//│ └──────────────────────────┘ │
//└──────────────────────────────┘

// head chunk 结构
//┌─────────────────────┬───────────────────────┬───────────────────────┬───────────────────┬───────────────┬──────────────┬────────────────┐
//| series ref <8 byte> | mint <8 byte, uint64> | maxt <8 byte, uint64> | encoding <1 byte> | len <uvarint> | data <bytes> │ CRC32 <4 byte> │
//└─────────────────────┴───────────────────────┴───────────────────────┴───────────────────┴───────────────┴──────────────┴────────────────┘

// 传入 head chunk file 索引序号（高四字节）和 chunk 在文件中的字节偏移量（低四字节），构造成一个 head chunk 在磁盘上的位置
func newChunkDiskMapperRef(seq, offset uint64) ChunkDiskMapperRef {
	return ChunkDiskMapperRef((seq << 32) | offset)
}

// Unpack 把 head chunk 在磁盘上的位置解析成 head chunk 索引和 chunk 偏移量
func (ref ChunkDiskMapperRef) Unpack() (seq, offset int) {
	seq = int(ref >> 32)
	offset = int((ref << 32) >> 32)
	return seq, offset
}

// GreaterThanOrEqualTo 判断当前 ChunkDiskMapperRef 是否 大于等于 给定的 r 的位置，先比较高四字节的索引，若相等再比较偏移量，若更大为 true
func (ref ChunkDiskMapperRef) GreaterThanOrEqualTo(r ChunkDiskMapperRef) bool {
	s1, o1 := ref.Unpack()
	s2, o2 := r.Unpack()
	return s1 > s2 || (s1 == s2 && o1 >= o2)
}

// GreaterThan 判断当前 ChunkDiskMapperRef 是否 大于 给定的 r 的位置，先比较高四字节的索引，若相等再比较偏移量，若更大为 true
func (ref ChunkDiskMapperRef) GreaterThan(r ChunkDiskMapperRef) bool {
	s1, o1 := ref.Unpack()
	s2, o2 := r.Unpack()
	return s1 > s2 || (s1 == s2 && o1 > o2)
}

// CorruptionErr is an error that's returned when corruption is encountered.
// 遇到崩溃时返回该 Error，实现了 error 接口，可以直接构造该类型的错误并返回
type CorruptionErr struct {
	Dir       string // head chunk file 路径
	FileIndex int    // head chunk file 索引（序号）
	Err       error
}

func (e *CorruptionErr) Error() string {
	return fmt.Errorf("corruption in head chunk file %s: %w", segmentFile(e.Dir, e.FileIndex), e.Err).Error()
}

func (e *CorruptionErr) Unwrap() error {
	return e.Err
}

// chunkPos keeps track of the position in the head chunk files.
// chunkPos is not thread-safe, a lock must be used to protect it.
// 记录 chunk 在 head chunk files 中的位置，不是线程安全的，要加锁
type chunkPos struct {
	seq     uint64 // Index of chunk file.		head chunk file 索引序号
	offset  uint64 // Offset within chunk file.	chunk 在 head chunk file 中的字节偏移量	两者共同构造一个 ChunkDiskMapperRef
	cutFile bool   // When true then the next chunk will be written to a new file.	// 若为 true，表示下一个 chunk 要写入新的 head chunk file 中
}

// getNextChunkRef takes a chunk and returns the chunk reference which will refer to it once it has been written.
// getNextChunkRef also decides whether a new file should be cut before writing this chunk, and it returns the decision via the second return value.
// The order of calling getNextChunkRef must be the order in which chunks are written to the disk.
// 传入一个 chunk，返回当该 chunk 被写入磁盘时要用到的 ChunkDiskMapperRef；会在把 chunk 写入磁盘之前决定是否切换到新文件 （cutFile）；调用该函数的顺序必须和 chunk 被写入磁盘的顺序一致
func (f *chunkPos) getNextChunkRef(chk chunkenc.Chunk) (chkRef ChunkDiskMapperRef, cutFile bool) {
	chkLen := uint64(len(chk.Bytes()))             // 有效数据总长
	bytesToWrite := f.bytesToWriteForChunk(chkLen) // chunk 中所有数据的总长，包括所有元数据

	// 若 chunk 是否需要写入新的文件中
	if f.shouldCutNewFile(bytesToWrite) {
		f.toNewFile() // 更新 head chunk file 的元数据
		f.cutFile = false
		cutFile = true // 向上层返回，表示需要切换文件
	}

	chkOffset := f.offset    // 该 chunk 的起始地址偏移量
	f.offset += bytesToWrite // 下一个 chunk 的偏移量

	// 用文件序号和偏移量构造 reference
	return newChunkDiskMapperRef(f.seq, chkOffset), cutFile
}

// toNewFile updates the seq/offset position to point to the beginning of a new chunk file.
// 切换到新的 head chunk file ，更新序号和偏移量（8 byte）
func (f *chunkPos) toNewFile() {
	f.seq++
	f.offset = SegmentHeaderSize
}

// cutFileOnNextChunk triggers that the next chunk will be written in to a new file.
// Not thread safe, a lock must be held when calling this.
// 表明下一个 chunk 需要写入到新的 head chunk file 中，不是线程安全的，要加锁
func (f *chunkPos) cutFileOnNextChunk() {
	f.cutFile = true
}

// setSeq sets the sequence number of the head chunk file.
// 为 chunk 所在的 head chunk file 指定序号
func (f *chunkPos) setSeq(seq uint64) {
	f.seq = seq
}

// shouldCutNewFile returns whether a new file should be cut based on the file size.
// Not thread safe, a lock must be held when calling this.
// 根据 chunk 的数据量判断是否需要切换到新文件
func (f *chunkPos) shouldCutNewFile(bytesToWrite uint64) bool {
	if f.cutFile { // 已经提前指明需要切换
		return true
	}

	// 需要创建第一个 head chunk file 或当前剩余空间写不下
	// offset == 0 ：如果不是第一个文件，offset 至少有 header 数据的字节数，不会是 0，只有还没有第一个文件时才会是 0
	return f.offset == 0 || // First head chunk file.
		f.offset+bytesToWrite > MaxHeadChunkFileSize // Exceeds the max head chunk file size.
}

// bytesToWriteForChunk returns the number of bytes that will need to be written for the given chunk size,
// including all meta data before and after the chunk data.
// Head chunk format: https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/head_chunks.md#chunk
// 返回对于给定的 chunk size （data length）需要写入磁盘的字节数量，包括所有元数据（seriesRef 8，mint 8，maxt 8，encoding 1，len uvarint 5，data，CRC32 4 bytes）
func (f *chunkPos) bytesToWriteForChunk(chkLen uint64) uint64 {
	// Headers.
	bytes := uint64(SeriesRefSize) + 2*MintMaxtSize + ChunkEncodingSize

	// Size of chunk length encoded as uvarint.		数据长度使用变长编码
	bytes += uint64(varint.UvarintSize(chkLen))

	// Chunk length.
	bytes += chkLen

	// crc32.
	bytes += CRCSize

	return bytes
}

// ChunkDiskMapper is for writing the Head block chunks to disk
// and access chunks via mmapped files.
// 用于把 head chunks 写入磁盘，并通过 mmapped files 访问 chunks 在内存中映射的数据
type ChunkDiskMapper struct {
	// Writer.
	dir             *os.File // chunks_head 目录路径
	writeBufferSize int      // 64 KB 到 8 MB 之间，必须是 1024 的整数倍

	curFile         *os.File      // File being written to.		当前正在写入的 head chunk file
	curFileSequence int           // Index of current open file being appended to. 0 if no file is active.	当前正在写入的文件的序号（索引）
	curFileOffset   atomic.Uint64 // Bytes written in current open file.	// 当前文件已经写入的字节数
	curFileMaxt     int64         // Used for the size retention.	// 当前文件中的 chunks 的最大时间戳

	// The values in evtlPos represent the file position which will eventually be
	// reached once the content of the write queue has been fully processed.
	evtlPosMtx sync.Mutex // 锁
	evtlPos    chunkPos   // head chunk file 全部处理完成后，最终要到达的 chunk 的位置（最后的文件的序号和 chunk 在其中的偏移量）

	byteBuf      [MaxHeadChunkMetaSize]byte // Buffer used to write the header of the chunk.	// 用于写入 chunk header 34 byte 的 buffer
	chkWriter    *bufio.Writer              // Writer for the current open file.	// 当前打开的文件的 chunk Writer buffer，写满了之后会自动把数据刷到底层的 io.Writer
	crc32        hash.Hash
	writePathMtx sync.Mutex // 把 chunk 写入磁盘时使用

	// Reader.
	// The int key in the map is the file number on the disk.
	// map 中的 key(int) 是磁盘上的文件序号
	mmappedChunkFiles map[int]*mmappedChunkFile // Contains the m-mapped files for each chunk file mapped with its index.	目录下所有 head chunk file 的 mmap file 映射；	用 文件序号 映射到 每个 chunk file 的 mmap file(序号->内存中的字节流)
	closers           map[int]io.Closer         // Closers for resources behind the byte slices.	// 实现的 io.Closer 接口，释放资源；每个 mmap 文件都有一个 closer
	readPathMtx       sync.RWMutex              // Mutex used to protect the above 2 maps.		// 保护上面两个 map 的锁
	pool              chunkenc.Pool             // This is used when fetching a chunk from the disk to allocate a chunk.	// chunk pool，在从磁盘中读取一个 chunk 时为其在内存中分配空间

	// Writer and Reader.
	// We flush chunks to disk in batches. Hence, we store them in this buffer
	// from which chunks are served till they are flushed and are ready for m-mapping.
	// chunks 批量刷入磁盘，用于存放 chkWriter buffer 中的 chunks 的 ref->chunk 的映射，直到它们被刷入磁盘并且准备好 mmap 了
	chunkBuffer *chunkBuffer

	// Whether the maxt field is set for all mmapped chunk files tracked within the mmappedChunkFiles map.
	// This is done after iterating through all the chunks in those files using the IterateAllChunks method.
	// 所有在 mmapedChuknFiles 中映射的 mmapped chunk files 的 maxt 字段是否均已设置；
	// maxt 字段在用 IterateAllChunks 方法遍历这些文件中的所有 chunks 之后被设置
	fileMaxtSet bool

	writeQueue *chunkWriteQueue // chunk 的 write 队列，默认没有

	closed bool // 当前目录的 ChunkDiskMapper 是否已关闭
}

// mmappedChunkFile provides mmap access to an entire head chunks file that holds many chunks.
// 映射到内存中的文件（head chunk file），byteSlice []byte 是文件的数据
// 为 存储了多个 chunks 的 head chunks file 的访问提供 mmap
type mmappedChunkFile struct {
	byteSlice ByteSlice // []byte，所有 chunk 的数据的字节流
	maxt      int64     // Max timestamp among all of this file's chunks.	所有 chunk 的最大时间戳
}

// NewChunkDiskMapper returns a new ChunkDiskMapper against the given directory
// using the default head chunk file duration.
// NOTE: 'IterateAllChunks' method needs to be called at least once after creating ChunkDiskMapper
// to set the maxt of all files.
// 为给定的目录创建一个新的 ChunkDiskMapper，对目录下的文件进行内存映射，使用默认的 head chunk file duration；创建完成后至少要调用一次 IterateAllChunks 方法来设置所有文件的 maxt 字段
func NewChunkDiskMapper(reg prometheus.Registerer, dir string, pool chunkenc.Pool, writeBufferSize, writeQueueSize int) (*ChunkDiskMapper, error) {
	// Validate write buffer size.
	// ChunkDiskMapper.writeBufferSize
	if writeBufferSize < MinWriteBufferSize || writeBufferSize > MaxWriteBufferSize {
		return nil, fmt.Errorf("ChunkDiskMapper write buffer size should be between %d and %d (actual: %d)", MinWriteBufferSize, MaxWriteBufferSize, writeBufferSize)
	}
	// buffer size 必须是 1024 的整数倍
	if writeBufferSize%1024 != 0 {
		return nil, fmt.Errorf("ChunkDiskMapper write buffer size should be a multiple of 1024 (actual: %d)", writeBufferSize)
	}

	// 创建 dir 目录，赋予所有权限
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir) // 打开目录，提供写权限
	if err != nil {
		return nil, err
	}

	// 构造 CHunkDiskMapper 结构
	m := &ChunkDiskMapper{
		dir:             dirFile, // 目录
		pool:            pool,
		writeBufferSize: writeBufferSize,
		crc32:           newCRC32(),
		chunkBuffer:     newChunkBuffer(), // 创建一个 chunkBuffer，ref->chunk 的映射
	}

	if writeQueueSize > 0 { // 默认没有
		m.writeQueue = newChunkWriteQueue(reg, writeQueueSize, m.writeChunk) // 创建 write queue
	}

	if m.pool == nil {
		m.pool = chunkenc.NewPool()
	}

	return m, m.openMMapFiles()
}

// Chunk encodings for out-of-order chunks.
// These encodings must be only used by the Head block for its internal bookkeeping.
// 用于乱序 chunks 的 encoding 的掩码，只能由 head block 用于其内部记录
const (
	OutOfOrderMask = uint8(0b10000000)
)

// 获取一个乱序 chunk 的编码，EncXOR == uint8(1)
func (cdm *ChunkDiskMapper) ApplyOutOfOrderMask(sourceEncoding chunkenc.Encoding) chunkenc.Encoding {
	enc := uint8(sourceEncoding) | OutOfOrderMask
	return chunkenc.Encoding(enc)
}

// IsOutOfOrderChunk 判断是否是乱序 chunk 的编码，经过 ApplyOutOfOrderMask 的编码之后结果为 true
func (cdm *ChunkDiskMapper) IsOutOfOrderChunk(e chunkenc.Encoding) bool {
	return (uint8(e) & OutOfOrderMask) != 0
}

// 从编码中移除乱序掩码，无论是否使用了乱序掩码，结果都一样
func (cdm *ChunkDiskMapper) RemoveMasks(sourceEncoding chunkenc.Encoding) chunkenc.Encoding {
	restored := uint8(sourceEncoding) & (^OutOfOrderMask)
	return chunkenc.Encoding(restored)
}

// openMMapFiles opens all files within dir for mmapping.
// 打开目录 dir 下的所有文件，对其进行 mmap
func (cdm *ChunkDiskMapper) openMMapFiles() (returnErr error) {
	cdm.mmappedChunkFiles = map[int]*mmappedChunkFile{} // key 是文件序号
	cdm.closers = map[int]io.Closer{}
	// 在函数返回前调用
	defer func() {
		if returnErr != nil {
			returnErr = tsdb_errors.NewMulti(returnErr, closeAllFromMap(cdm.closers)).Err()

			cdm.mmappedChunkFiles = nil
			cdm.closers = nil
		}
	}()

	// 找到目录下所有 head chunk file 的文件的路径
	files, err := listChunkFiles(cdm.dir.Name())
	if err != nil {
		return err
	}

	// 删除在系统崩溃时可能产生的最后一个空文件
	files, err = repairLastChunkFile(files)
	if err != nil {
		return err
	}

	chkFileIndices := make([]int, 0, len(files)) // 存储 head chunk files index
	for seq, fn := range files {
		f, err := fileutil.OpenMmapFile(fn) // 进行内存映射
		if err != nil {
			return fmt.Errorf("mmap files, file: %s: %w", fn, err)
		}
		cdm.closers[seq] = f                                                                // 为该文件设置 closer （MmapFile 类型 实现了 Close() 方法，可以用作 io.Closer 类型）
		cdm.mmappedChunkFiles[seq] = &mmappedChunkFile{byteSlice: realByteSlice(f.Bytes())} // head chunk file 序号和 数据字节流 的映射
		chkFileIndices = append(chkFileIndices, seq)
	}

	// Check for gaps in the files.
	slices.Sort(chkFileIndices) // 为文件序号排序
	if len(chkFileIndices) == 0 {
		return nil
	}
	// 确保所有 head chunk file 的序号都是连续的
	lastSeq := chkFileIndices[0]
	for _, seq := range chkFileIndices[1:] {
		if seq != lastSeq+1 {
			return fmt.Errorf("found unsequential head chunk files %s (index: %d) and %s (index: %d)", files[lastSeq], lastSeq, files[seq], seq)
		}
		lastSeq = seq
	}

	// 验证每个内存映射文件的 header 数据是否正确
	for i, b := range cdm.mmappedChunkFiles {
		// 文件是否完整（有完整的 header）
		if b.byteSlice.Len() < HeadChunkFileHeaderSize {
			return fmt.Errorf("%s: invalid head chunk file header: %w", files[i], errInvalidSize)
		}
		// Verify magic number.
		// 验证 header magic
		if m := binary.BigEndian.Uint32(b.byteSlice.Range(0, MagicChunksSize)); m != MagicHeadChunks {
			return fmt.Errorf("%s: invalid magic number %x", files[i], m)
		}

		// Verify chunk format version.
		// 验证 version
		if v := int(b.byteSlice.Range(MagicChunksSize, MagicChunksSize+ChunksFormatVersionSize)[0]); v != chunksFormatV1 {
			return fmt.Errorf("%s: invalid chunk format version %d", files[i], v)
		}
	}

	// 设置处理完成后最后的文件序号
	cdm.evtlPos.setSeq(uint64(lastSeq))

	return nil
}

// 找到目录下所有文件名是六位数字格式的 head chunk file，把序号（文件名的数字）和文件路径（dir/file_name）存为 map
func listChunkFiles(dir string) (map[int]string, error) {
	files, err := os.ReadDir(dir) // 读取目录下的所有文件
	if err != nil {
		return nil, err
	}
	// 找到所有文件名是六位数字格式的 head chunk file，把序号（文件名的数字）和文件路径（dir/file_name）存为 map
	res := map[int]string{}
	for _, fi := range files {
		seq, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}
		res[int(seq)] = filepath.Join(dir, fi.Name())
	}

	return res, nil
}

// HardLinkChunkFiles creates hardlinks for chunk files from src to dst.
// It does nothing if src doesn't exist and ensures dst is created if not.
// 为 src 目录下的所有 chunk files 在 dst 目录下创建硬链接；若目录不存在就直接返回
// 硬链接允许多个文件名指向同一文件数据，任何对文件的修改都会反映在所有硬链接上
func HardLinkChunkFiles(src, dst string) error {
	// 确认文件存在
	_, err := os.Stat(src)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check source chunks dir: %w", err)
	}
	// 创建 dst 目录
	if err := os.MkdirAll(dst, 0o777); err != nil {
		return fmt.Errorf("set up destination chunks dir: %w", err)
	}
	files, err := listChunkFiles(src) // 找到 src 目录下的所有 head chunk file，存为 map
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}
	for _, filePath := range files { // 文件路径
		_, fileName := filepath.Split(filePath)                                    // 拆分出文件名
		err := os.Link(filepath.Join(src, fileName), filepath.Join(dst, fileName)) // 在 dst 目录下创建硬链接
		if err != nil {
			return fmt.Errorf("hardlink a chunk: %w", err)
		}
	}
	return nil
}

// repairLastChunkFile deletes the last file if it's empty.
// Because we don't fsync when creating these files, we could end
// up with an empty file at the end during an abrupt shutdown.
// 如果最后一个文件是空的就删除：因为创建文件时没有同步，系统突然崩溃时可能在最后产生空文件
func repairLastChunkFile(files map[int]string) (_ map[int]string, returnErr error) {
	// 找到文件的最大序号
	lastFile := -1
	for seq := range files {
		if seq > lastFile {
			lastFile = seq
		}
	}

	// 没有文件
	if lastFile <= 0 {
		return files, nil
	}

	// 打开最后一个文件
	f, err := os.Open(files[lastFile])
	if err != nil {
		return files, fmt.Errorf("open file during last head chunk file repair: %w", err)
	}

	buf := make([]byte, MagicChunksSize)
	size, err := f.Read(buf) // 从文件中读取 4 byte magic
	if err != nil && !errors.Is(err, io.EOF) {
		return files, fmt.Errorf("failed to read magic number during last head chunk file repair: %w", err)
	}
	if err := f.Close(); err != nil {
		return files, fmt.Errorf("close file during last head chunk file repair: %w", err)
	}

	// We either don't have enough bytes for the magic number or the magic number is 0.
	// NOTE: we should not check for wrong magic number here because that error
	// needs to be sent up the function called (already done elsewhere)
	// for proper repair mechanism to happen in the Head.
	// magic 不足四字节或 值为 0，表示需要删除
	if size < MagicChunksSize || binary.BigEndian.Uint32(buf) == 0 {
		// Corrupt file, hence remove it.
		// 删除该文件
		if err := os.RemoveAll(files[lastFile]); err != nil {
			return files, fmt.Errorf("delete corrupted, empty head chunk file during last file repair: %w", err)
		}
		delete(files, lastFile) // 从 files map 中删除 lastFile 的映射
	}

	return files, nil
}

// WriteChunk writes the chunk to disk.
// The returned chunk ref is the reference from where the chunk encoding starts for the chunk.
// 把 chunk 写入磁盘，返回的 chunk ref 是 chunk 编码的起始位置，该函数内部必须使用 ChunkDiskMapper.evtlPosMtx 锁
func (cdm *ChunkDiskMapper) WriteChunk(seriesRef HeadSeriesRef, mint, maxt int64, chk chunkenc.Chunk, isOOO bool, callback func(err error)) (chkRef ChunkDiskMapperRef) {
	// cdm.evtlPosMtx must be held to serialize the calls to cdm.evtlPos.getNextChunkRef() and the writing of the chunk (either with or without queue).
	cdm.evtlPosMtx.Lock()
	defer cdm.evtlPosMtx.Unlock()
	ref, cutFile := cdm.evtlPos.getNextChunkRef(chk) // 获取该 chunk 写入时对应的 ref，以及是否需要切换到新文件

	// 通过 write queue 写入磁盘
	if cdm.writeQueue != nil {
		return cdm.writeChunkViaQueue(ref, isOOO, cutFile, seriesRef, mint, maxt, chk, callback)
	}

	// 直接把 chunk 写入磁盘
	err := cdm.writeChunk(seriesRef, mint, maxt, chk, ref, isOOO, cutFile)
	// 使用回调函数处理异常
	if callback != nil {
		callback(err)
	}

	return ref
}

// 通过 write queue 写入磁盘
func (cdm *ChunkDiskMapper) writeChunkViaQueue(ref ChunkDiskMapperRef, isOOO, cutFile bool, seriesRef HeadSeriesRef, mint, maxt int64, chk chunkenc.Chunk, callback func(err error)) (chkRef ChunkDiskMapperRef) {
	var err error
	// 用回调函数处理异常
	if callback != nil {
		defer func() {
			if err != nil {
				callback(err)
			}
		}()
	}

	// 加入队列等待处理
	err = cdm.writeQueue.addJob(chunkWriteJob{
		cutFile:   cutFile,
		seriesRef: seriesRef,
		mint:      mint,
		maxt:      maxt,
		chk:       chk,
		ref:       ref,
		isOOO:     isOOO,
		callback:  callback,
	})

	return ref
}

// 直接把 chunk 写入磁盘，内部需要锁； chunkWriteQueue 也使用这个函数
func (cdm *ChunkDiskMapper) writeChunk(seriesRef HeadSeriesRef, mint, maxt int64, chk chunkenc.Chunk, ref ChunkDiskMapperRef, isOOO, cutFile bool) (err error) {
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()

	if cdm.closed {
		return ErrChunkDiskMapperClosed
	}

	// 若需要切换到新文件，创建其 mmap file，验证序号是否和 ref 提供的一致
	if cutFile {
		err := cdm.cutAndExpectRef(ref)
		if err != nil {
			return err
		}
	}

	// if len(chk.Bytes())+MaxHeadChunkMetaSize >= writeBufferSize, it means that chunk >= the buffer size;
	// so no need to flush here, as we have to flush at the end (to not keep partial chunks in buffer).
	// 若当前 chunk 能放进完整的 chunkWriter buffer，但是当前 buffer 的剩余空间不够了，先把之前的 chunks 落盘，避免在 buffer 中存储一部分 chunk
	if len(chk.Bytes())+MaxHeadChunkMetaSize < cdm.writeBufferSize && cdm.chkWriter.Available() < MaxHeadChunkMetaSize+len(chk.Bytes()) {
		if err := cdm.flushBuffer(); err != nil {
			return err
		}
	}

	cdm.crc32.Reset()
	bytesWritten := 0

	// 向 byteBuffer 存入 head chunk file header
	binary.BigEndian.PutUint64(cdm.byteBuf[bytesWritten:], uint64(seriesRef))
	bytesWritten += SeriesRefSize
	binary.BigEndian.PutUint64(cdm.byteBuf[bytesWritten:], uint64(mint))
	bytesWritten += MintMaxtSize
	binary.BigEndian.PutUint64(cdm.byteBuf[bytesWritten:], uint64(maxt))
	bytesWritten += MintMaxtSize
	enc := chk.Encoding()
	if isOOO { // 若是乱序 chunk，计算其 encoding
		enc = cdm.ApplyOutOfOrderMask(enc)
	}
	cdm.byteBuf[bytesWritten] = byte(enc)
	bytesWritten += ChunkEncodingSize
	n := binary.PutUvarint(cdm.byteBuf[bytesWritten:], uint64(len(chk.Bytes()))) // data length 使用变长编码，最多 5 bytes
	bytesWritten += n

	// 分别向 chunk buffer 写入 header ，data，crc
	if err := cdm.writeAndAppendToCRC32(cdm.byteBuf[:bytesWritten]); err != nil {
		return err
	}
	if err := cdm.writeAndAppendToCRC32(chk.Bytes()); err != nil {
		return err
	}
	if err := cdm.writeCRC32(); err != nil {
		return err
	}

	// 更新当前文件的最大时间
	if maxt > cdm.curFileMaxt {
		cdm.curFileMaxt = maxt
	}

	// 存储 chunkWriter buffer 中的这个 ref->chk 的映射
	cdm.chunkBuffer.put(ref, chk)

	// 若 chunk 大小超过 chunkWriter buffer 容量，把 buffer 中此时剩余的数据刷入底层的 io.Writer
	if len(chk.Bytes())+MaxHeadChunkMetaSize >= cdm.writeBufferSize {
		// The chunk was bigger than the buffer itself.
		// Flushing to not keep partial chunks in buffer.
		if err := cdm.flushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

// CutNewFile makes that a new file will be created the next time a chunk is written.
// 表示下一个 chunk 写入时需要切换到新文件，只修改了 cdm.cutNewFile
func (cdm *ChunkDiskMapper) CutNewFile() {
	cdm.evtlPosMtx.Lock()
	defer cdm.evtlPosMtx.Unlock()

	cdm.evtlPos.cutFileOnNextChunk()
}

// write queue 是否为空
func (cdm *ChunkDiskMapper) IsQueueEmpty() bool {
	if cdm.writeQueue == nil {
		return true
	}

	return cdm.writeQueue.queueIsEmpty()
}

// cutAndExpectRef creates a new m-mapped file.
// The write lock should be held before calling this.
// It ensures that the position in the new file matches the given chunk reference, if not then it errors.
// 创建一个新的 mmapped file，确保新文件的位置和给定的 chunk reference （文件序号+文件内偏移量）一致；锁在上层调用时加上了
func (cdm *ChunkDiskMapper) cutAndExpectRef(chkRef ChunkDiskMapperRef) (err error) {
	seq, offset, err := cdm.cut() // 创建一个新的 mmap file，cdm 状态切换到新文件上
	if err != nil {
		return err
	}

	// 比较创建的文件的 序号和偏移量 是否和 chunk reference 中指定的一致
	if expSeq, expOffset := chkRef.Unpack(); seq != expSeq || offset != expOffset {
		return fmt.Errorf("expected newly cut file to have sequence:offset %d:%d, got %d:%d", expSeq, expOffset, seq, offset)
	}

	return nil
}

// cut creates a new m-mapped file. The write lock should be held before calling this.
// It returns the file sequence and the offset in that file to start writing chunks.
// 创建一个新的 mmapped file，返回文件序号和文件内偏移量，以此开始写入 chunk；锁在上层调用时加上了
func (cdm *ChunkDiskMapper) cut() (seq, offset int, returnErr error) {
	// Sync current tail to disk and close.
	// 当前文件（末尾文件）存入磁盘并关闭
	if err := cdm.finalizeCurFile(); err != nil {
		return 0, 0, err
	}

	// 切换到新的 head chunk file，写入 header，offset == headerSize 8 byte
	offset, newFile, seq, err := cutSegmentFile(cdm.dir, MagicHeadChunks, headChunksFormatV1, HeadChunkFilePreallocationSize)
	if err != nil {
		return 0, 0, err
	}

	defer func() {
		// The file should not be closed if there is no error,
		// its kept open in the ChunkDiskMapper.
		if returnErr != nil {
			returnErr = tsdb_errors.NewMulti(returnErr, newFile.Close()).Err()
		}
	}()

	cdm.curFileOffset.Store(uint64(offset)) // 原子化存储当前文件中已写入的字节数

	// 若当前文件没有关闭，读取并存储其中的最大时间戳
	if cdm.curFile != nil {
		cdm.readPathMtx.Lock()
		cdm.mmappedChunkFiles[cdm.curFileSequence].maxt = cdm.curFileMaxt
		cdm.readPathMtx.Unlock()
	}

	// 对新文件进行内存映射
	mmapFile, err := fileutil.OpenMmapFileWithSize(newFile.Name(), MaxHeadChunkFileSize)
	if err != nil {
		return 0, 0, err
	}

	cdm.readPathMtx.Lock()
	cdm.curFileSequence = seq // 修改当前文件为新文件
	cdm.curFile = newFile
	if cdm.chkWriter != nil { // 重置 chunkWriter
		cdm.chkWriter.Reset(newFile)
	} else {
		cdm.chkWriter = bufio.NewWriterSize(newFile, cdm.writeBufferSize)
	}

	cdm.closers[cdm.curFileSequence] = mmapFile                                                                // 文件对应的 closer
	cdm.mmappedChunkFiles[cdm.curFileSequence] = &mmappedChunkFile{byteSlice: realByteSlice(mmapFile.Bytes())} // 文件序号到字节流数据的映射
	cdm.readPathMtx.Unlock()

	cdm.curFileMaxt = 0

	return seq, offset, nil
}

// finalizeCurFile writes all pending data to the current tail file,
// truncates its size, and closes it.
// 把所有待写入数据写入当前末尾的文件，裁剪文件大小（去除末尾空余空间），关闭文件
func (cdm *ChunkDiskMapper) finalizeCurFile() error {
	// 当前没有在写入的文件
	if cdm.curFile == nil {
		return nil
	}

	// 当前内存 buffer 的 chunks 全部写入 io.Writer，重置 chunkBuffer
	if err := cdm.flushBuffer(); err != nil {
		return err
	}

	// 当前文件的数据存入磁盘
	if err := cdm.curFile.Sync(); err != nil {
		return err
	}

	// 关闭当前文件
	return cdm.curFile.Close()
}

// 向 chunkWriter buffer 写入数据
func (cdm *ChunkDiskMapper) write(b []byte) error {
	n, err := cdm.chkWriter.Write(b)
	cdm.curFileOffset.Add(uint64(n)) // 文件中已写入的字节数
	return err
}

// 向 chunkWriter buffer 写入数据，并把数据写入 cdm.crc32
func (cdm *ChunkDiskMapper) writeAndAppendToCRC32(b []byte) error {
	if err := cdm.write(b); err != nil {
		return err
	}
	_, err := cdm.crc32.Write(b)
	return err
}

// 向 chunkWriter buffer 写入 crc32
func (cdm *ChunkDiskMapper) writeCRC32() error {
	return cdm.write(cdm.crc32.Sum(cdm.byteBuf[:0]))
}

// flushBuffer flushes the current in-memory chunks.
// Assumes that writePathMtx is _write_ locked before calling this method.
// 把当前内存 buffer 中的 chunks 刷入 io.Writer ，重置 chunkBuffer
func (cdm *ChunkDiskMapper) flushBuffer() error {
	if err := cdm.chkWriter.Flush(); err != nil {
		return err
	}
	cdm.chunkBuffer.clear()
	return nil
}

// Chunk returns a chunk from a given reference.
// 为给定的 ChunkDiskMapperRef 返回其对应的 chunk
func (cdm *ChunkDiskMapper) Chunk(ref ChunkDiskMapperRef) (chunkenc.Chunk, error) {
	cdm.readPathMtx.RLock()
	// We hold this read lock for the entire duration because if Close()
	// is called, the data in the byte slice will get corrupted as the mmapped
	// file will be closed.
	defer cdm.readPathMtx.RUnlock()

	if cdm.closed {
		return nil, ErrChunkDiskMapperClosed
	}

	// 若 write queue 不为空，尝试从中获取目标 chunk
	if cdm.writeQueue != nil {
		chunk := cdm.writeQueue.get(ref)
		if chunk != nil {
			return chunk, nil
		}
	}

	// 解析出文件索引（序号）和chunk在文件内的偏移量
	sgmIndex, chkStart := ref.Unpack()
	// We skip the series ref and the mint/maxt beforehand.
	// 跳过该chunk的series ref、mint、maxt，此时chunk的起始位置是 encoding
	chkStart += SeriesRefSize + (2 * MintMaxtSize)

	// If it is the current open file, then the chunks can be in the buffer too.
	// 若目标文件就是当前正在写入的文件，则目标 chunk 也可能在 chkWrite buffer 中，在 map 查找
	if sgmIndex == cdm.curFileSequence {
		chunk := cdm.chunkBuffer.get(ref)
		if chunk != nil {
			return chunk, nil
		}
	}

	// 找到该文件序号对应的内存中的 mmap file
	mmapFile, ok := cdm.mmappedChunkFiles[sgmIndex]
	// 若没找到
	if !ok {
		// 若目标序号大于当前正在写入的序号
		if sgmIndex > cdm.curFileSequence {
			return nil, &CorruptionErr{
				Dir:       cdm.dir.Name(),
				FileIndex: -1,
				Err:       fmt.Errorf("head chunk file index %d more than current open file", sgmIndex),
			}
		}
		// 目标文件不存在
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       fmt.Errorf("head chunk file index %d does not exist on disk", sgmIndex),
		}
	}

	// 若 encooding+len 字段没有完整读取
	if chkStart+MaxChunkLengthFieldSize > mmapFile.byteSlice.Len() {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       fmt.Errorf("head chunk file doesn't include enough bytes to read the chunk size data field - required:%v, available:%v", chkStart+MaxChunkLengthFieldSize, mmapFile.byteSlice.Len()),
		}
	}

	// Encoding.
	// 获取文件中 chunk 的编码方式字段
	chkEnc := mmapFile.byteSlice.Range(chkStart, chkStart+ChunkEncodingSize)[0]
	sourceChkEnc := chunkenc.Encoding(chkEnc)
	// Extract the encoding from the byte. ChunkDiskMapper uses only the last 7 bits for the encoding.
	// 获取 chunk 的原始编码方式（可能因为是乱序 chunk 而使用了 OutOfOrderMask ）
	chkEnc = byte(cdm.RemoveMasks(sourceChkEnc))
	// Data length.
	// With the minimum chunk length this should never cause us reading
	// over the end of the slice.
	// 获取 chunk 的数据长度
	chkDataLenStart := chkStart + ChunkEncodingSize // len 的起始位置
	c := mmapFile.byteSlice.Range(chkDataLenStart, chkDataLenStart+MaxChunkLengthFieldSize)
	chkDataLen, n := binary.Uvarint(c)
	if n <= 0 {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       fmt.Errorf("reading chunk length failed with %d", n),
		}
	}

	// Verify the chunk data end.
	// 验证数据部分的末尾
	chkDataEnd := chkDataLenStart + n + int(chkDataLen)
	if chkDataEnd > mmapFile.byteSlice.Len() {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       fmt.Errorf("head chunk file doesn't include enough bytes to read the chunk - required:%v, available:%v", chkDataEnd, mmapFile.byteSlice.Len()),
		}
	}

	// Check the CRC.
	// 检查校验和
	sum := mmapFile.byteSlice.Range(chkDataEnd, chkDataEnd+CRCSize)
	if err := checkCRC32(mmapFile.byteSlice.Range(chkStart-(SeriesRefSize+2*MintMaxtSize), chkDataEnd), sum); err != nil {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       err,
		}
	}

	// The chunk data itself.
	// 获取 chunk 的数据部分
	chkData := mmapFile.byteSlice.Range(chkDataEnd-int(chkDataLen), chkDataEnd)

	// Make a copy of the chunk data to prevent a panic occurring because the returned
	// chunk data slice references an mmap-ed file which could be closed after the
	// function returns but while the chunk is still in use.
	// 获取数据的副本，因为数据来自 mmap file，文件可能被关闭，而 chunk 仍然在被使用，就访问不到数据了
	chkDataCopy := make([]byte, len(chkData))
	copy(chkDataCopy, chkData) // 数据副本

	chk, err := cdm.pool.Get(chunkenc.Encoding(chkEnc), chkDataCopy) // 用数据副本从 chunk pool 中获取并构造一个 chunk 结构
	if err != nil {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       err,
		}
	}
	return chk, nil
}

// IterateAllChunks iterates all mmappedChunkFiles (in order of head chunk file name/number) and all the chunks within it
// and runs the provided function with information about each chunk. It returns on the first error encountered.
// NOTE: This method needs to be called at least once after creating ChunkDiskMapper
// to set the maxt of all files.
// 按文件序号迭代所有 mmap file 和文件中的所有 chunks，对所有 chunk 调用回调函数；返回遇到的第一个错误
// 该方法在创建 ChunkDiskMapper 之后至少要调用一次，用于设置所有文件的最大时间 maxt
func (cdm *ChunkDiskMapper) IterateAllChunks(f func(seriesRef HeadSeriesRef, chunkRef ChunkDiskMapperRef, mint, maxt int64, numSamples uint16, encoding chunkenc.Encoding, isOOO bool) error) (err error) {
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()

	defer func() {
		cdm.fileMaxtSet = true
	}()

	// Iterate files in ascending order.
	// 升序 迭代所有文件，获取序号
	segIDs := make([]int, 0, len(cdm.mmappedChunkFiles))
	for seg := range cdm.mmappedChunkFiles {
		segIDs = append(segIDs, seg)
	}
	slices.Sort(segIDs)
	// 遍历所有 head chunk file
	for _, segID := range segIDs {
		mmapFile := cdm.mmappedChunkFiles[segID] // 序号对应的 mmap file
		fileEnd := mmapFile.byteSlice.Len()      // 文件末尾（文件字节数）
		if segID == cdm.curFileSequence {
			fileEnd = int(cdm.curFileSize())
		}
		idx := HeadChunkFileHeaderSize // 用于遍历文件内容的索引，跳过 head chunk file header 8 byte
		// 遍历当前文件的所有 chunk
		for idx < fileEnd {
			if fileEnd-idx < MaxHeadChunkMetaSize { // 文件中没有完整的 chunk header
				// Check for all 0s which marks the end of the file.
				// 检查是否全为 0（表示文件结束）
				allZeros := true
				for _, b := range mmapFile.byteSlice.Range(idx, fileEnd) {
					if b != byte(0) { // 有不为零的数据，表示文件读取有异常
						allZeros = false
						break
					}
				}
				if allZeros { // 若全为 0，跳过这个文件
					// End of segment chunk file content.
					break
				}
				return &CorruptionErr{ // 若读取异常，直接返回
					Dir:       cdm.dir.Name(),
					FileIndex: segID,
					Err: fmt.Errorf("head chunk file has some unread data, but doesn't include enough bytes to read the chunk header"+
						" - required:%v, available:%v, file:%d", idx+MaxHeadChunkMetaSize, fileEnd, segID),
				}
			}
			// 用文件序号和 chunk 偏移量构造一个 chunkRef
			chunkRef := newChunkDiskMapperRef(uint64(segID), uint64(idx))

			startIdx := idx // chunk header 起始位置
			// 读取 chunk header 的元数据
			seriesRef := HeadSeriesRef(binary.BigEndian.Uint64(mmapFile.byteSlice.Range(idx, idx+SeriesRefSize)))
			idx += SeriesRefSize
			mint := int64(binary.BigEndian.Uint64(mmapFile.byteSlice.Range(idx, idx+MintMaxtSize)))
			idx += MintMaxtSize
			maxt := int64(binary.BigEndian.Uint64(mmapFile.byteSlice.Range(idx, idx+MintMaxtSize)))
			idx += MintMaxtSize

			// We preallocate file to help with m-mapping (especially windows systems).
			// As series ref always starts from 1, we assume it being 0 to be the end of the actual file data.
			// We are not considering possible file corruption that can cause it to be 0.
			// Additionally we are checking mint and maxt just to be sure.
			// 若当前文件中的当前位置没有 chunk 了，退出该文件的遍历
			if seriesRef == 0 && mint == 0 && maxt == 0 {
				break
			}

			// 继续读取元数据
			chkEnc := chunkenc.Encoding(mmapFile.byteSlice.Range(idx, idx+ChunkEncodingSize)[0])
			idx += ChunkEncodingSize
			dataLen, n := binary.Uvarint(mmapFile.byteSlice.Range(idx, idx+MaxChunkLengthFieldSize))
			idx += n

			// 数据部分的前两位表示 chunk 中的 sample 数量，之后是时间戳和value
			numSamples := binary.BigEndian.Uint16(mmapFile.byteSlice.Range(idx, idx+2))
			idx += int(dataLen) // Skip the data.

			// In the beginning we only checked for the chunk meta size.
			// Now that we have added the chunk data length, we check for sufficient bytes again.
			// 检查是否有完整的 crc
			if idx+CRCSize > fileEnd {
				return &CorruptionErr{
					Dir:       cdm.dir.Name(),
					FileIndex: segID,
					Err:       fmt.Errorf("head chunk file doesn't include enough bytes to read the chunk header - required:%v, available:%v, file:%d", idx+CRCSize, fileEnd, segID),
				}
			}

			// Check CRC.
			// 校验 crc
			sum := mmapFile.byteSlice.Range(idx, idx+CRCSize)
			if err := checkCRC32(mmapFile.byteSlice.Range(startIdx, idx), sum); err != nil {
				return &CorruptionErr{
					Dir:       cdm.dir.Name(),
					FileIndex: segID,
					Err:       err,
				}
			}
			idx += CRCSize

			// 更新该 mmap file 的最大时间
			if maxt > mmapFile.maxt {
				mmapFile.maxt = maxt
			}
			// 判断是否是乱序chunk
			isOOO := cdm.IsOutOfOrderChunk(chkEnc)
			// Extract the encoding from the byte. ChunkDiskMapper uses only the last 7 bits for the encoding.
			// 获取 chunk 的原始编码方式
			chkEnc = cdm.RemoveMasks(chkEnc)
			// 对该 chunk 调用回调函数
			if err := f(seriesRef, chunkRef, mint, maxt, numSamples, chkEnc, isOOO); err != nil {
				var cerr *CorruptionErr
				if errors.As(err, &cerr) {
					cerr.Dir = cdm.dir.Name()
					cerr.FileIndex = segID
					return cerr
				}
				return err
			}
			// 一个 chunk 处理完成
		}

		// 一个文件处理完成退出循环到这里时， idx == fileEnd
		if idx > fileEnd {
			// It should be equal to the slice length.
			return &CorruptionErr{
				Dir:       cdm.dir.Name(),
				FileIndex: segID,
				Err:       fmt.Errorf("head chunk file doesn't include enough bytes to read the last chunk data - required:%v, available:%v, file:%d", idx, fileEnd, segID),
			}
		}
	}

	return nil
}

// Truncate deletes the head chunk files with numbers less than the given fileNo.
// 删除目录下所有小于给定序号的 head chunk files
func (cdm *ChunkDiskMapper) Truncate(fileNo uint32) error {
	cdm.readPathMtx.RLock()

	// Sort the file indices, else if files deletion fails in between,
	// it can lead to unsequential files as the map is not sorted.
	// 获取文件序号并排序
	chkFileIndices := make([]int, 0, len(cdm.mmappedChunkFiles))
	for seq := range cdm.mmappedChunkFiles {
		chkFileIndices = append(chkFileIndices, seq)
	}
	slices.Sort(chkFileIndices)

	// 找到所有待删除文件的编号
	var removedFiles []int
	for _, seq := range chkFileIndices {
		if seq == cdm.curFileSequence || uint32(seq) >= fileNo { // 若是当前在写入的文件（末尾文件）或不小于待删除编号，退出
			break
		}
		removedFiles = append(removedFiles, seq)
	}
	cdm.readPathMtx.RUnlock()

	errs := tsdb_errors.NewMulti()
	// Cut a new file only if the current file has some chunks.
	// 若当前 file 中已经存了 chunks，则下一个 chunk 需要存入新的文件中，因为当前文件也可能会被删除
	if cdm.curFileSize() > HeadChunkFileHeaderSize {
		// There is a known race condition here because between the check of curFileSize() and the call to CutNewFile()
		// a new file could already be cut, this is acceptable because it will simply result in an empty file which
		// won't do any harm.
		cdm.CutNewFile() // 指明接下来的 chunk 要存入新文件
	}
	pendingDeletes, err := cdm.deleteFiles(removedFiles) // 删除选定的文件
	errs.Add(err)

	// 若目录下的所有文件都被删除了
	if len(chkFileIndices) == len(removedFiles) {
		// All files were deleted. Reset the current sequence.
		cdm.evtlPosMtx.Lock()

		// We can safely reset the sequence only if the write queue is empty. If it's not empty,
		// then there may be a job in the queue that will create a new segment file with an ID
		// generated before the sequence reset.
		//
		// The queueIsEmpty() function must be called while holding the cdm.evtlPosMtx to avoid
		// a race condition with WriteChunk().
		// 只有 write queue 为空时才重置序号
		if cdm.writeQueue == nil || cdm.writeQueue.queueIsEmpty() {
			if err == nil {
				cdm.evtlPos.setSeq(0) // 序号归零
			} else {
				// In case of error, set it to the last file number on the disk that was not deleted.
				cdm.evtlPos.setSeq(uint64(pendingDeletes[len(pendingDeletes)-1])) // 磁盘上的最后一个没有被删除的文件
			}
		}

		cdm.evtlPosMtx.Unlock()
	}

	return errs.Err()
}

// deleteFiles deletes the given file sequences in order of the sequence.
// In case of an error, it returns the sorted file sequences that were not deleted from the _disk_.
// 给定待删除的文件序号，从磁盘中当前目录下删除这些文件，返回删除失败的文件的数量（只要失败就立即返回，不继续尝试删除其他文件）
func (cdm *ChunkDiskMapper) deleteFiles(removedFiles []int) ([]int, error) {
	slices.Sort(removedFiles) // To delete them in order.
	cdm.readPathMtx.Lock()
	for _, seq := range removedFiles {
		// 删除前先关闭文件
		if err := cdm.closers[seq].Close(); err != nil {
			cdm.readPathMtx.Unlock()
			return removedFiles, err
		}
		// 从 map 中删除相应元素
		delete(cdm.mmappedChunkFiles, seq)
		delete(cdm.closers, seq)
	}
	cdm.readPathMtx.Unlock()

	// We actually delete the files separately to not block the readPathMtx for long.
	// 依次构建文件路径并删除文件
	for i, seq := range removedFiles {
		if err := os.Remove(segmentFile(cdm.dir.Name(), seq)); err != nil {
			return removedFiles[i:], err
		}
	}

	return nil, nil
}

// DeleteCorrupted deletes all the head chunk files after the one which had the corruption
// (including the corrupt file).
// 删除崩溃的文件及其之后的所有  head chunk files
func (cdm *ChunkDiskMapper) DeleteCorrupted(originalErr error) error {
	var cerr *CorruptionErr
	if !errors.As(originalErr, &cerr) {
		return fmt.Errorf("cannot handle error: %w", originalErr)
	}

	// Delete all the head chunk files following the corrupt head chunk file.
	// 找到所有序号大于等于崩溃的文件序号的所有文件
	segs := []int{}
	cdm.readPathMtx.RLock()
	lastSeq := 0
	for seg := range cdm.mmappedChunkFiles {
		switch {
		case seg >= cerr.FileIndex: // 待删除文件
			segs = append(segs, seg)
		case seg > lastSeq: // 完好的文件，记录其序号
			lastSeq = seg
		}
	}
	cdm.readPathMtx.RUnlock()

	pendingDeletes, err := cdm.deleteFiles(segs) // 删除失败的文件数量
	cdm.evtlPosMtx.Lock()
	if err == nil {
		cdm.evtlPos.setSeq(uint64(lastSeq)) // 若删除成功，设置为当前的最后一个有效的文件序号
	} else {
		// In case of error, set it to the last file number on the disk that was not deleted.
		// 若删除失败，设置为磁盘上没有被删除的最后一个文件的序号
		cdm.evtlPos.setSeq(uint64(pendingDeletes[len(pendingDeletes)-1]))
	}
	cdm.evtlPosMtx.Unlock()

	return err
}

// Size returns the size of the chunk files.
// 获取该目录下的所有 head chunk files 的总字节数
func (cdm *ChunkDiskMapper) Size() (int64, error) {
	return fileutil.DirSize(cdm.dir.Name())
}

// 当前文件中的字节数
func (cdm *ChunkDiskMapper) curFileSize() uint64 {
	return cdm.curFileOffset.Load() // 原子性地读取一个值
}

// Close closes all the open files in ChunkDiskMapper.
// It is not longer safe to access chunks from this struct after calling Close.
// 关闭目录下所有打开的文件，清理数据，释放资源
func (cdm *ChunkDiskMapper) Close() error {
	// Locking the eventual position lock blocks WriteChunk()
	cdm.evtlPosMtx.Lock()
	defer cdm.evtlPosMtx.Unlock()

	// 关闭 write queue
	if cdm.writeQueue != nil {
		cdm.writeQueue.stop()
	}

	// 'WriteChunk' locks writePathMtx first and then readPathMtx for cutting head chunk file.
	// The lock order should not be reversed here else it can cause deadlocks.
	// 如果这里两个锁的顺序调转，Close 函数可能会和 WriteChunk 函数分别持有读锁和写锁，由于申请不到另一个锁导致函数不能退出，持有的锁无法释放，进而导致死锁
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()
	cdm.readPathMtx.Lock()
	defer cdm.readPathMtx.Unlock()

	// 设置 ChunkDiskMapper 的状态为已关闭
	if cdm.closed {
		return nil
	}
	cdm.closed = true

	errs := tsdb_errors.NewMulti(
		closeAllFromMap(cdm.closers), // 关闭所有 Closer
		cdm.finalizeCurFile(),        // 把所有待写入数据写入末尾文件，裁剪并关闭末尾文件
		cdm.dir.Close(),              // 关闭目录
	)
	cdm.mmappedChunkFiles = map[int]*mmappedChunkFile{} // 清空 map
	cdm.closers = map[int]io.Closer{}

	return errs.Err()
}

// 关闭所有 mmap files 的 Closer
func closeAllFromMap(cs map[int]io.Closer) error {
	errs := tsdb_errors.NewMulti()
	for _, c := range cs {
		errs.Add(c.Close())
	}
	return errs.Err()
}

const inBufferShards = 128 // 128 is a randomly chosen number.

// chunkBuffer is a thread safe lookup table for chunks by their ref.
// 用 ChunkDiskMapperRef 映射一个 chunk，线程安全；	shardIdx = ref % inBufferShards，把 map 用 ref 散列到数组中，减少单个 map 的元素，提升查询效率
// 把所有 ref map 划分成 128 个 shards
type chunkBuffer struct {
	inBufferChunks     [inBufferShards]map[ChunkDiskMapperRef]chunkenc.Chunk // 长度为 128 的数组，数组元素为 map，从 ref 映射到 chunk
	inBufferChunksMtxs [inBufferShards]sync.RWMutex                          // 锁
}

// 创建一个 chunkBuffer，ref->chunk 的映射
func newChunkBuffer() *chunkBuffer {
	cb := &chunkBuffer{}
	for i := 0; i < inBufferShards; i++ {
		cb.inBufferChunks[i] = make(map[ChunkDiskMapperRef]chunkenc.Chunk)
	}
	return cb
}

// 把 ref 和 chx 存入 shard map；	shardIdx := ref % inBufferShards （128）
func (cb *chunkBuffer) put(ref ChunkDiskMapperRef, chk chunkenc.Chunk) {
	shardIdx := ref % inBufferShards // 选取 shard

	cb.inBufferChunksMtxs[shardIdx].Lock() // 需要加锁
	cb.inBufferChunks[shardIdx][ref] = chk // 存入 map
	cb.inBufferChunksMtxs[shardIdx].Unlock()
}

// 用 ref 查询一个在 chkWriter buffer 中的 chunk
func (cb *chunkBuffer) get(ref ChunkDiskMapperRef) chunkenc.Chunk {
	shardIdx := ref % inBufferShards

	cb.inBufferChunksMtxs[shardIdx].RLock() // 读锁，可以并发读取，不能写入
	defer cb.inBufferChunksMtxs[shardIdx].RUnlock()

	return cb.inBufferChunks[shardIdx][ref]
}

// 清除所有 shard 中的 map
func (cb *chunkBuffer) clear() {
	for i := 0; i < inBufferShards; i++ {
		cb.inBufferChunksMtxs[i].Lock()
		cb.inBufferChunks[i] = make(map[ChunkDiskMapperRef]chunkenc.Chunk)
		cb.inBufferChunksMtxs[i].Unlock()
	}
}

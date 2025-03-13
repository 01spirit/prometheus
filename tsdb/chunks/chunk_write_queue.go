// Copyright 2021 The Prometheus Authors
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
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	// Minimum recorded peak since the last shrinking of chunkWriteQueue.chunkRefMap to shrink it again.
	// 触发 chunkRefMap 缩小的界限
	chunkRefMapShrinkThreshold = 1000

	// Minimum interval between shrinking of chunkWriteQueue.chunkRefMap.
	// 两次缩小的最小时间间隔
	chunkRefMapMinShrinkInterval = 10 * time.Minute

	// Maximum size of segment used by job queue (number of elements). With chunkWriteJob being 64 bytes,
	// this will use ~512 KiB for empty queue.
	// 任务队列中一个 segment 中的最大任务数量
	maxChunkQueueSegmentSize = 8192
)

// 一个向磁盘写入 chunk 的任务，包含了 chunk 的元数据和应该写在磁盘上的位置 ChunkDiskMapperRef
type chunkWriteJob struct {
	cutFile   bool          // 是否切换文件
	seriesRef HeadSeriesRef //  series reference 时间序列的指针
	mint      int64         // 最小时间
	maxt      int64         // 最大时间
	chk       chunkenc.Chunk
	ref       ChunkDiskMapperRef // 一个 head chunk 在磁盘上的位置：高四字节是 head chunk file 的索引序号，低四字节是 chunk 起始位置在 head chunk file 中的字节偏移量
	isOOO     bool               // 是否是乱序 chunk
	callback  func(error)        // 回调函数
}

// chunkWriteQueue is a queue for writing chunks to disk in a non-blocking fashion.
// Chunks that shall be written get added to the queue, which is consumed asynchronously.
// Adding jobs to the queue is non-blocking as long as the queue isn't full.
// 用于以非阻塞的方式把 chunks 写入磁盘的队列；待写入磁盘的 chunks 被异步添加到队列中；只要队列未满，向其中添加任务就是非阻塞的
type chunkWriteQueue struct {
	jobs *writeJobQueue // chunk 写入任务的队列

	chunkRefMapMtx        sync.RWMutex
	chunkRefMap           map[ChunkDiskMapperRef]chunkenc.Chunk // chunk 在磁盘上的索引 到 内存中的 chunk 的映射
	chunkRefMapPeakSize   int                                   // Largest size that chunkRefMap has grown to since the last time we shrank it.	自从上次缩小chunkRefMap以来，它已经增长到了最大的大小
	chunkRefMapLastShrink time.Time                             // When the chunkRefMap has been shrunk the last time.	chunkRefMap 最后一次缩小的时间

	// isRunningMtx serves two purposes:
	// 1. It protects isRunning field.
	// 2. It serializes adding of jobs to the chunkRefMap in addJob() method. If jobs channel is full then addJob() will block
	// while holding this mutex, which guarantees that chunkRefMap won't ever grow beyond the queue size + 1.
	// 可以用于避免写入队列的任务数量超出队列的 maxSize
	isRunningMtx sync.Mutex
	isRunning    bool // Used to prevent that new jobs get added to the queue when the chan is already closed.	防止当管道关闭时有新任务被写入队列

	workerWg sync.WaitGroup // 用于协调 goroutine

	writeChunk writeChunkF // 写入 chunks 的函数

	// Keeping separate counters instead of only a single CounterVec to improve the performance of the critical
	// addJob() method which otherwise would need to perform a WithLabelValues call on the CounterVec.
	adds      prometheus.Counter // 操作计数
	gets      prometheus.Counter
	completed prometheus.Counter
	shrink    prometheus.Counter
}

// writeChunkF is a function which writes chunks, it is dynamic to allow mocking in tests.
// 用于写入 chunks 的函数
type writeChunkF func(HeadSeriesRef, int64, int64, chunkenc.Chunk, ChunkDiskMapperRef, bool, bool) error

// 创建一个新的写入队列
func newChunkWriteQueue(reg prometheus.Registerer, size int, writeChunk writeChunkF) *chunkWriteQueue {
	// 用于构造 counter
	counters := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_tsdb_chunk_write_queue_operations_total",
			Help: "Number of operations on the chunk_write_queue.",
		},
		[]string{"operation"},
	)

	segmentSize := size
	if segmentSize > maxChunkQueueSegmentSize {
		segmentSize = maxChunkQueueSegmentSize
	}

	// 构造
	q := &chunkWriteQueue{
		jobs:                  newWriteJobQueue(size, segmentSize), // 创建写任务队列
		chunkRefMap:           make(map[ChunkDiskMapperRef]chunkenc.Chunk),
		chunkRefMapLastShrink: time.Now(),
		writeChunk:            writeChunk,

		adds:      counters.WithLabelValues("add"),
		gets:      counters.WithLabelValues("get"),
		completed: counters.WithLabelValues("complete"),
		shrink:    counters.WithLabelValues("shrink"),
	}

	if reg != nil {
		reg.MustRegister(counters)
	}

	q.start() // 启动队列，处理任务
	return q
}

// 启动队列，开始处理任务
func (c *chunkWriteQueue) start() {
	c.workerWg.Add(1) // waitGroup 计数
	go func() {       // 开启一个 goroutine
		defer c.workerWg.Done() // 结束计数

		for {
			job, ok := c.jobs.pop() // 从队列中读取任务
			if !ok {
				return
			}

			c.processJob(job) // 处理任务
		}
	}()

	c.isRunningMtx.Lock()
	c.isRunning = true
	c.isRunningMtx.Unlock()
}

// 处理写入任务
func (c *chunkWriteQueue) processJob(job chunkWriteJob) {
	err := c.writeChunk(job.seriesRef, job.mint, job.maxt, job.chk, job.ref, job.isOOO, job.cutFile) // 向磁盘写入 chunk
	if job.callback != nil {                                                                         // 用回调函数处理异常
		job.callback(err)
	}

	c.chunkRefMapMtx.Lock()
	defer c.chunkRefMapMtx.Unlock()

	delete(c.chunkRefMap, job.ref) // 从 map 中删除处理完的 chunk

	c.completed.Inc() // 计数

	c.shrinkChunkRefMap() // 尝试缩小 chunkRefMap
}

// shrinkChunkRefMap checks whether the conditions to shrink the chunkRefMap are met,
// if so chunkRefMap is reinitialized. The chunkRefMapMtx must be held when calling this method.
//
// We do this because Go runtime doesn't release internal memory used by map after map has been emptied.
// To achieve that we create new map instead and throw the old one away.
// 检验是否满足缩小 chunkRefMap 的条件（map 为空 或 PeakSize >= ShrinkThreshold 或 两次 shrink 时间间隔太短），若满足则重新初始化 map
// 由于 Go runtime 不会释放已经空了的 map 使用的内存，需要创建一个新的 map 替代
func (c *chunkWriteQueue) shrinkChunkRefMap() {
	// 若 map 不为空，不能 shrink
	if len(c.chunkRefMap) > 0 {
		// Can't shrink it while there is data in it.
		return
	}

	// map 峰值大小未满足 shrink 界限
	if c.chunkRefMapPeakSize < chunkRefMapShrinkThreshold {
		// Not shrinking it because it has not grown to the minimum threshold yet.
		return
	}

	now := time.Now()

	// 时间间隔太短
	if now.Sub(c.chunkRefMapLastShrink) < chunkRefMapMinShrinkInterval {
		// Not shrinking it because the minimum duration between shrink-events has not passed yet.
		return
	}

	// Re-initialize the chunk ref map to half of the peak size that it has grown to since the last re-init event.
	// We are trying to hit the sweet spot in the trade-off between initializing it to a very small size
	// potentially resulting in many allocations to re-grow it, and initializing it to a large size potentially
	// resulting in unused allocated memory.
	// 重新创建为上次创建后的峰值大小的一半
	c.chunkRefMap = make(map[ChunkDiskMapperRef]chunkenc.Chunk, c.chunkRefMapPeakSize/2)

	c.chunkRefMapPeakSize = 0
	c.chunkRefMapLastShrink = now
	c.shrink.Inc()
}

// 向队列中添加任务
func (c *chunkWriteQueue) addJob(job chunkWriteJob) (err error) {
	defer func() {
		if err == nil {
			c.adds.Inc() // 计数
		}
	}()

	c.isRunningMtx.Lock()
	defer c.isRunningMtx.Unlock()

	if !c.isRunning {
		return errors.New("queue is not running")
	}

	c.chunkRefMapMtx.Lock()
	c.chunkRefMap[job.ref] = job.chk // 保存映射

	// Keep track of the peak usage of c.chunkRefMap.
	// map 峰值大小
	if len(c.chunkRefMap) > c.chunkRefMapPeakSize {
		c.chunkRefMapPeakSize = len(c.chunkRefMap)
	}
	c.chunkRefMapMtx.Unlock()

	// 加入队列（满时阻塞等待），若失败，，从 map 中删除
	if ok := c.jobs.push(job); !ok {
		c.chunkRefMapMtx.Lock()
		delete(c.chunkRefMap, job.ref)
		c.chunkRefMapMtx.Unlock()

		return errors.New("queue is closed")
	}

	return nil
}

// 用 chunk 磁盘索引从 chunkRefMap 中获取对应的 chunk
func (c *chunkWriteQueue) get(ref ChunkDiskMapperRef) chunkenc.Chunk {
	c.chunkRefMapMtx.RLock()
	defer c.chunkRefMapMtx.RUnlock()

	chk, ok := c.chunkRefMap[ref]
	if ok {
		c.gets.Inc() // 计数
	}

	return chk
}

// 停止写入队列，等待所有 goroutine 退出
func (c *chunkWriteQueue) stop() {
	c.isRunningMtx.Lock()
	defer c.isRunningMtx.Unlock()

	if !c.isRunning {
		return
	}

	c.isRunning = false

	c.jobs.close()

	c.workerWg.Wait()
}

// 队列是否为空
func (c *chunkWriteQueue) queueIsEmpty() bool {
	return c.queueSize() == 0
}

// 队列是否已满
func (c *chunkWriteQueue) queueIsFull() bool {
	// When the queue is full and blocked on the writer the chunkRefMap has one more job than the cap of the jobCh
	// because one job is currently being processed and blocked in the writer.
	return c.queueSize() == c.jobs.maxSize+1
}

// 当前队列中的 chunk 数量
func (c *chunkWriteQueue) queueSize() int {
	c.chunkRefMapMtx.Lock()
	defer c.chunkRefMapMtx.Unlock()

	// Looking at chunkRefMap instead of jobCh because the job is popped from the chan before it has
	// been fully processed, it remains in the chunkRefMap until the processing is complete.
	return len(c.chunkRefMap)
}

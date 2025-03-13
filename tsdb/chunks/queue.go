// Copyright 2022 The Prometheus Authors
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

import "sync"

// writeJobQueue is similar to buffered channel of chunkWriteJob, but manages its own buffers
// to avoid using a lot of memory when it's empty. It does that by storing elements into segments
// of equal size (segmentSize). When segment is not used anymore, reference to it are removed,
// so it can be treated as a garbage.
// chunk 写入磁盘的任务的队列；队列划分为 segment 管理，一个 segment 中包含了一组 chunkWriteJob，所有 segment 内的元素数量相同，segment 之间类似单链表节点的连接，指向下一个 segment
// 队列的 push 向最后一个 segment 的数组中添加 job ，存满后扩充到下一个 segment ；pop 从第一个 segment 的数组中从前往后读取 job，全读完后从队列中移除第一个 segment
type writeJobQueue struct {
	maxSize     int // 队列元素最大数量
	segmentSize int // 一个 segment 中的元素数量

	mtx            sync.Mutex            // protects all following variables	队列元素的操作需要加锁
	pushed, popped *sync.Cond            // signalled when something is pushed into the queue or popped from it		出入队列时的信号量
	first, last    *writeJobQueueSegment // pointer to first and last segment, if any	队列的起始和末尾元素（segment）
	size           int                   // total size of the queue		队列实际总长度
	closed         bool                  // after closing the queue, nothing can be pushed to it	队列是否关闭，关闭之后不能再 push 了
}

// 写入任务队列的分段 segment
type writeJobQueueSegment struct {
	segment             []chunkWriteJob       // 一个 segment 是一组待写入的 chunk
	nextRead, nextWrite int                   // index of next read and next write in this segment.		段中的下一个读（pop）和写（push）的序号（segment 数组索引）
	nextSegment         *writeJobQueueSegment // next segment, if any	队列中的下一个 segment
}

// 创建一个新的写入任务队列，指定队列的最大元素数量和每个 segment 的最大元素数量，初始化信号量
func newWriteJobQueue(maxSize, segmentSize int) *writeJobQueue {
	if maxSize <= 0 || segmentSize <= 0 {
		panic("invalid queue")
	}

	q := &writeJobQueue{
		maxSize:     maxSize,
		segmentSize: segmentSize,
	}

	// 初始化信号量
	q.pushed = sync.NewCond(&q.mtx)
	q.popped = sync.NewCond(&q.mtx)
	return q
}

// 关闭队列，释放信号量，使相应的 goroutine 退出阻塞状态
func (q *writeJobQueue) close() {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.closed = true

	// Unblock all blocked goroutines.
	q.pushed.Broadcast()
	q.popped.Broadcast()
}

// push blocks until there is space available in the queue, and then adds job to the queue.
// If queue is closed or gets closed while waiting for space, push returns false.
// 向队列中添加 chunkWriteJob 任务；阻塞，直到队列中有空位
func (q *writeJobQueue) push(job chunkWriteJob) bool {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	// Wait until queue has more space or is closed.
	// 阻塞直到信号表明队列中有空位或队列已关闭
	for !q.closed && q.size >= q.maxSize {
		q.popped.Wait()
	}

	if q.closed {
		return false
	}

	// Check if this segment has more space for writing, and create new one if not.
	// 若当前的 segment 不可以写入
	if q.last == nil || q.last.nextWrite >= q.segmentSize {
		prevLast := q.last
		// 创建一个新的 segment
		q.last = &writeJobQueueSegment{
			segment: make([]chunkWriteJob, q.segmentSize),
		}

		// 前一个节点的指针指向新的 segment
		if prevLast != nil {
			prevLast.nextSegment = q.last
		}
		if q.first == nil { // 若是队列中的首个 segment
			q.first = q.last
		}
	}

	// 添加任务，更新状态和信号
	q.last.segment[q.last.nextWrite] = job
	q.last.nextWrite++
	q.size++
	q.pushed.Signal()
	return true
}

// pop returns first job from the queue, and true.
// If queue is empty, pop blocks until there is a job (returns true), or until queue is closed (returns false).
// If queue was already closed, pop first returns all remaining elements from the queue (with true value), and only then returns false.
// 返回队列中的第一个任务；若队列为空，阻塞直到有任务
func (q *writeJobQueue) pop() (chunkWriteJob, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	// wait until something is pushed to the queue, or queue is closed.
	// 阻塞直到队列中有任务，或队列已关闭
	for q.size == 0 {
		if q.closed {
			return chunkWriteJob{}, false
		}

		q.pushed.Wait()
	}

	// 第一个 segment 的第一个 chunkWriteJob
	res := q.first.segment[q.first.nextRead]
	q.first.segment[q.first.nextRead] = chunkWriteJob{} // clear just-read element	清空队列中读取的任务（不会再写入这里）
	q.first.nextRead++
	q.size-- // 队列中的当前任务数

	// If we have read all possible elements from first segment, we can drop it.
	// 当前 segment 的所有任务都被 pop 了，从队列中移除这个 segment
	if q.first.nextRead >= q.segmentSize {
		q.first = q.first.nextSegment
		if q.first == nil {
			q.last = nil
		}
	}

	q.popped.Signal()
	return res, true
}

// length returns number of all jobs in the queue.
// 队列中当前任务数量
func (q *writeJobQueue) length() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	return q.size
}

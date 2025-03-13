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

package index

import (
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/bboreham/go-loser"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// 用于保存倒排索引中的所有 seriesID ；Label name 和 value 都是空字符串
var allPostingsKey = labels.Label{}

// AllPostingsKey returns the label key that is used to store the postings list of all existing IDs.
// 返回用于存储了倒排索引中所有 seriesID 的 Label key（所有可能的标签键值的组合）
func AllPostingsKey() (name, value string) {
	return allPostingsKey.Name, allPostingsKey.Value
}

// ensureOrderBatchSize is the max number of postings passed to a worker in a single batch in MemPostings.EnsureOrder().
const ensureOrderBatchSize = 1024

// ensureOrderBatchPool is a pool used to recycle batches passed to workers in MemPostings.EnsureOrder().
var ensureOrderBatchPool = sync.Pool{
	New: func() interface{} {
		x := make([][]storage.SeriesRef, 0, ensureOrderBatchSize)
		return &x // Return pointer type as preferred by Pool.
	},
}

// MemPostings holds postings list for series ID per label pair. They may be written
// to out of order.
// EnsureOrder() must be called once before any reads are done. This allows for quick
// unordered batch fills on startup.
// 内存中的倒排索引
// MemPostings 维护了由 每个标签对 到其 series ID 的倒排索引，这些索引写入时可能是无序的
//
//	一个 MemPostings 有多个 Label，一个 Label Key 对应多个 Value（多个 value 存为一个 map），一个 Value 对应多个 SeriesRef
//
// 在首次读取 MemPostings 之前，必须调用 EnsureOrder() ，这允许在启动时快速无序批量填充，之后再排序
type MemPostings struct {
	mtx     sync.RWMutex                              // 读写锁，用于同步多个 goroutine 的并发访问，允许同时读，禁止同时写
	m       map[string]map[string][]storage.SeriesRef // Label Key -> map[ Label Value -> []SeriesRef]
	ordered bool                                      // 是否已排序
}

// NewMemPostings returns a memPostings that's ready for reads and writes.
// 创建一个 MemPostings ，设置为 ordered，允许读和写
func NewMemPostings() *MemPostings {
	return &MemPostings{
		m:       make(map[string]map[string][]storage.SeriesRef, 512),
		ordered: true,
	}
}

// NewUnorderedMemPostings returns a memPostings that is not safe to be read from
// until EnsureOrder() was called once.
// 创建一个 MemPostings ，设置为 unordered，读是不安全的，直到调用一次 EnsureOrder()
func NewUnorderedMemPostings() *MemPostings {
	return &MemPostings{
		m:       make(map[string]map[string][]storage.SeriesRef, 512), // 指定初始容量，之后可以自动扩容
		ordered: false,
	}
}

// Symbols returns an iterator over all unique name and value strings, in order.
// 获取 MemPostings 中的所有 Label key 和 value 的字符串（不做区分，存在一起），去重，已排序，构造成一个字符串迭代器
func (p *MemPostings) Symbols() StringIter {
	p.mtx.RLock()

	// Add all the strings to a map to de-duplicate.
	// 把 MemPostings.m 中的所有 Label key 和 value 都存入 symbols map
	// 用 map 是为了去除重复的字符串，key 和 value 都存为 symbols 的键
	// struct{} 是空结构体的类型，struct{}{} 是创建了一个空结构体的实例
	symbols := make(map[string]struct{}, 512) // 空结构体 struct{} 不占用内存空间，仅作为占位符
	for n, e := range p.m {                   // 先存入所有 Label key
		symbols[n] = struct{}{} // 映射的值设置为空结构体类型的实例
		for v := range e {      // 再存入所有 Label value
			symbols[v] = struct{}{}
		}
	}
	p.mtx.RUnlock() // MemPostings 完成读取，解锁

	// 取出存在 symbols 中的所有字符串
	res := make([]string, 0, len(symbols))
	for k := range symbols {
		res = append(res, k)
	}

	// 排序
	slices.Sort(res)
	return NewStringListIter(res)
}

// SortedKeys returns a list of sorted label keys of the postings.
// 返回 MemPostings 中的所有标签对，升序排列（先比较 name 再 value）
func (p *MemPostings) SortedKeys() []labels.Label {
	p.mtx.RLock()
	// 容量为 map 中 key 的数量
	keys := make([]labels.Label, 0, len(p.m))

	// 遍历 map，构造并保存所有标签对
	for n, e := range p.m {
		for v := range e {
			keys = append(keys, labels.Label{Name: n, Value: v})
		}
	}
	p.mtx.RUnlock()

	// 所有标签对按 name 和 value 升序排列
	slices.SortFunc(keys, func(a, b labels.Label) int {
		nameCompare := strings.Compare(a.Name, b.Name)
		// If names are the same, compare values.
		if nameCompare != 0 {
			return nameCompare
		}

		return strings.Compare(a.Value, b.Value)
	})
	return keys
}

// LabelNames returns all the unique label names.
// 返回 MemPostings 中的所有唯一的 label name
func (p *MemPostings) LabelNames() []string {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	n := len(p.m) // label name 数量
	if n == 0 {
		return nil
	}

	// 从 map 读取的 name 本来就不会重复，可以直接存进结果
	// 需要比较一下是不是 allPostingsKey（空的标签键值对）
	names := make([]string, 0, n-1) // 创建切片
	for name := range p.m {
		if name != allPostingsKey.Name {
			names = append(names, name)
		}
	}
	return names
}

// LabelValues returns label values for the given name.
// 返回给定 Label name 的所有 value
func (p *MemPostings) LabelValues(_ context.Context, name string) []string {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	// 遍历 map[name]
	values := make([]string, 0, len(p.m[name])) // 存放该 name 的所有 value
	for v := range p.m[name] {
		values = append(values, v)
	}
	return values
}

// PostingsStats contains cardinality based statistics for postings.
// 为 postings 提供基于基数的统计（Stat数组，Stat 保存的是单个基数的统计值）
type PostingsStats struct {
	CardinalityMetricsStats []Stat // 指定的 Label key 的 series 数量
	CardinalityLabelStats   []Stat // 每个 key 的 value 的数量
	LabelValueStats         []Stat // 每个 key 的 value-series 组合的数量
	LabelValuePairsStats    []Stat // 每个 Label 键值对 的 series 数量
	NumLabelPairs           int    // Labels 键值对的总数
}

// Stats calculates the cardinality statistics from postings.
// 计算 MemPostings 中的统计量
func (p *MemPostings) Stats(label string, limit int) *PostingsStats {
	var size uint64
	p.mtx.RLock()

	// 都用最大堆 maxHeap 保存最大的元素
	metrics := &maxHeap{}          // name 是 Label key，count 是 name 对应的 SeriesRef 数量
	labels := &maxHeap{}           // name 是 key，count 是 key 的 value 的数量
	labelValueLength := &maxHeap{} // name 是 key，count 是 key 的 value-series 组合的数量
	labelValuePairs := &maxHeap{}  // name 是一个 Label 键值对，count 是 value 对应的 SeriesRef 数量
	numLabelPairs := 0             // Labels 键值对的总数

	// 初始化最大堆，指定元素数量限制
	metrics.init(limit)
	labels.init(limit)
	labelValueLength.init(limit)
	labelValuePairs.init(limit)

	// n 是 Label key，e 是 map[Label value][]SeriesRef
	for n, e := range p.m {
		if n == "" { // allPostingsKey
			continue
		}
		labels.push(Stat{Name: n, Count: uint64(len(e))}) // 一个 Label key
		numLabelPairs += len(e)
		size = 0

		for name, values := range e {
			// 从所有 key 中找到指定的 name，统计为 metric
			if n == label {
				metrics.push(Stat{Name: name, Count: uint64(len(values))})
			}
			seriesCnt := uint64(len(values)) //  一个 Label value 对应的 series 数量
			labelValuePairs.push(Stat{Name: n + "=" + name, Count: seriesCnt})
			size += uint64(len(name)) * seriesCnt // 一个 key 下所有的 value-series 组合的数量
		}
		labelValueLength.push(Stat{Name: n, Count: size})
	}

	p.mtx.RUnlock()

	// 所有统计量的元素从大到小排列
	return &PostingsStats{
		CardinalityMetricsStats: metrics.get(),
		CardinalityLabelStats:   labels.get(),
		LabelValueStats:         labelValueLength.get(),
		LabelValuePairsStats:    labelValuePairs.get(),
		NumLabelPairs:           numLabelPairs,
	}
}

// Get returns a postings list for the given label pair.
// 返回给定标签对的倒排索引列表（对应的 series） []SeriesRef
func (p *MemPostings) Get(name, value string) Postings {
	var lp []storage.SeriesRef
	p.mtx.RLock()
	// 用标签对映射 map
	l := p.m[name]
	if l != nil {
		lp = l[value]
	}
	p.mtx.RUnlock()

	if lp == nil {
		return EmptyPostings()
	}
	return newListPostings(lp...)
}

// All returns a postings list over all documents ever added.
// 返回倒排索引中所有已存储的 series 的倒排索引列表
func (p *MemPostings) All() Postings {
	return p.Get(AllPostingsKey())
}

// EnsureOrder ensures that all postings lists are sorted. After it returns all further
// calls to add and addFor will insert new IDs in a sorted manner.
// Parameter numberOfConcurrentProcesses is used to specify the maximal number of
// CPU cores used for this operation. If it is <= 0, GOMAXPROCS is used.
// GOMAXPROCS was the default before introducing this parameter.
// 确保所有 postings lists [][]SeriesRef 都是有序的；该函数返回后，之后的所有 add 调用都会顺序插入
// 参数用于指定执行该函数的 CPU 核心数； <= 0 时使用所有当前可用的 CPU 核心（GOMAXPROCS）
func (p *MemPostings) EnsureOrder(numberOfConcurrentProcesses int) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	// 若已经是有序的，直接返回
	if p.ordered {
		return
	}

	// CPU 核心数量
	concurrency := numberOfConcurrentProcesses
	if concurrency <= 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}
	workc := make(chan *[][]storage.SeriesRef)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	// 开启协程，对 series 排序
	// 等待通道中有任务处理
	for i := 0; i < concurrency; i++ {
		go func() {
			for job := range workc {
				for _, l := range *job {
					slices.Sort(l)
				}

				// 清空任务队列，放回任务池
				*job = (*job)[:0]
				ensureOrderBatchPool.Put(job)
			}
			wg.Done()
		}()
	}

	// 向通道发送任务
	nextJob := ensureOrderBatchPool.Get().(*[][]storage.SeriesRef)
	for _, e := range p.m {
		for _, l := range e {
			*nextJob = append(*nextJob, l)

			// 分批次向通道中发送任务
			if len(*nextJob) >= ensureOrderBatchSize {
				workc <- nextJob
				nextJob = ensureOrderBatchPool.Get().(*[][]storage.SeriesRef)
			}
		}
	}

	// If the last job was partially filled, we need to push it to workers too.
	// 发送最后一个批次的任务
	if len(*nextJob) > 0 {
		workc <- nextJob
	}

	// 关闭通道，等待所有协程结束
	close(workc)
	wg.Wait()

	p.ordered = true
}

// Delete removes all ids in the given map from the postings lists.
// affectedLabels contains all the labels that are affected by the deletion, there's no need to check other labels.
// 从倒排索引中删除所有给定的 series，affected 包含了所有受到删除操作影响的标签，因此不需要检查除此之外的标签了
// struct{} 是用于占位的空结构，不占用实际内存，这里用 map 是为了避免传入重复元素，可以当作是个 set 结构
func (p *MemPostings) Delete(deleted map[storage.SeriesRef]struct{}, affected map[labels.Label]struct{}) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	// 定义处理删除操作的匿名函数
	process := func(l labels.Label) {
		orig := p.m[l.Name][l.Value]                    // 一个标签对应的 []SeriesRef
		repl := make([]storage.SeriesRef, 0, len(orig)) // 删除后剩下的 series
		for _, id := range orig {                       // 遍历这个标签下的所有 series，删除指定的 series
			if _, ok := deleted[id]; !ok {
				repl = append(repl, id)
			}
		}
		if len(repl) > 0 { // 若该标签的键值对应的 series 没有完全删除，保留剩余数据
			p.m[l.Name][l.Value] = repl
		} else { // 若没有剩余 series 了
			delete(p.m[l.Name], l.Value) // 删除该标签下以此 value 为键的 map
			// Delete the key if we removed all values.
			if len(p.m[l.Name]) == 0 { // 若删除 value map 之后 Label key 没有对应的 value 了，把 key map 也删除
				delete(p.m, l.Name)
			}
		}
	}

	// 遍历，处理所有受到删除影响的标签
	for l := range affected {
		process(l)
	}
	// 处理所有标签键值对的组合
	process(allPostingsKey)
}

// Iter calls f for each postings list. It aborts if f returns an error and returns it.
// 为每个倒排索引列表（SeriesRef）调用函数 f，遇到异常就终止
func (p *MemPostings) Iter(f func(labels.Label, Postings) error) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	// 遍历 MemPostings ，把每个标签键值对的 []SeriesRef 构造成倒排索引列表，作为参数传给函数 f
	for n, e := range p.m {
		for v, p := range e {
			if err := f(labels.Label{Name: n, Value: v}, newListPostings(p...)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Add a label set to the postings index.
// 向倒排索引（postings index）中添加一个 series 以及该序列拥有的所有 Label
func (p *MemPostings) Add(id storage.SeriesRef, lset labels.Labels) {
	p.mtx.Lock()

	// 遍历该 series 的所有标签，把 Label->series 的映射加入倒排索引
	lset.Range(func(l labels.Label) {
		p.addFor(id, l)
	})
	// 添加到 所有标签键值组合->series 的映射
	p.addFor(id, allPostingsKey)

	p.mtx.Unlock()
}

// 把 Label->series 的映射加入倒排索引；若倒排索引是有序的，需要按顺序插入（升序），就是把 list 数组排序
func (p *MemPostings) addFor(id storage.SeriesRef, l labels.Label) {
	nm, ok := p.m[l.Name] // label value->series 的映射
	// 若该 label value 没存进来过，创建该 value map
	if !ok {
		nm = map[string][]storage.SeriesRef{}
		p.m[l.Name] = nm
	}
	// 把 id 存入 value map 中的值（[]SeriesRef）
	list := append(nm[l.Value], id)
	nm[l.Value] = list

	// 若倒排索引此时是无序的，直接返回
	if !p.ordered {
		return
	}
	// There is no guarantee that no higher ID was inserted before as they may
	// be generated independently before adding them to postings.
	// We repair order violations on insert. The invariant is that the first n-1
	// items in the list are already sorted.
	// 若倒排索引是有序的，需要按顺序插入（升序），就是把 list 数组排序
	// 从后往前遍历交换元素
	for i := len(list) - 1; i >= 1; i-- {
		if list[i] >= list[i-1] {
			break
		}
		list[i], list[i-1] = list[i-1], list[i]
	}
}

// PostingsForLabelMatching 返回和给定 label name 匹配的所有 series 的 postings list
func (p *MemPostings) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) Postings {
	// We'll copy the values into a slice and then match over that,
	// this way we don't need to hold the mutex while we're matching,
	// which can be slow (seconds) if the match function is a huge regex.
	// Holding this lock prevents new series from being added (slows down the write path)
	// and blocks the compaction process.
	// label name 对应的所有 value
	vals := p.labelValues(name)
	// 遍历比较所有 value 是否匹配，若不匹配，把末尾元素覆盖到当前位置重新比较，最终 vals 中只剩下匹配的元素
	for i, count := 0, 1; i < len(vals); count++ {
		// 每隔 128 次循环检查是否异常
		if count%checkContextEveryNIterations == 0 && ctx.Err() != nil {
			return ErrPostings(ctx.Err())
		}

		// 若成功匹配，进入下一次循环
		if match(vals[i]) {
			i++
			continue
		}

		// Didn't match, bring the last value to this position, make the slice shorter and check again.
		// The order of the slice doesn't matter as it comes from a map iteration.
		// 若不匹配，把末尾元素移到当前位置重新比较
		vals[i], vals = vals[len(vals)-1], vals[:len(vals)-1]
	}

	// If none matched (or this label had no values), no need to grab the lock again.
	if len(vals) == 0 {
		return EmptyPostings()
	}

	// Now `vals` only contains the values that matched, get their postings.
	// 循环结束后 vals 中只剩成功匹配的元素，获取他们的 postings
	its := make([]Postings, 0, len(vals))
	p.mtx.RLock()
	e := p.m[name]
	// 取出 val 对应的 series，构造成 Postings 结构
	for _, v := range vals {
		if refs, ok := e[v]; ok {
			// Some of the values may have been garbage-collected in the meantime this is fine, we'll just skip them.
			// If we didn't let the mutex go, we'd have these postings here, but they would be pointing nowhere
			// because there would be a `MemPostings.Delete()` call waiting for the lock to delete these labels,
			// because the series were deleted already.
			its = append(its, NewListPostings(refs))
		}
	}
	// Let the mutex go before merging.
	p.mtx.RUnlock()

	// 所有 Postings 结构合并成一个 Postings
	return Merge(ctx, its...)
}

// labelValues returns a slice of label values for the given label name.
// It will take the read lock.
// 返回 MemPostings 中给定的 label name（key）对应的所有 label value
func (p *MemPostings) labelValues(name string) []string {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	e := p.m[name]
	if len(e) == 0 {
		return nil
	}

	vals := make([]string, 0, len(e))
	for v, srs := range e {
		if len(srs) > 0 {
			vals = append(vals, v)
		}
	}

	return vals
}

// ExpandPostings returns the postings expanded as a slice.
// 把倒排索引列表  Postings 构造成 []SeriesRef
func ExpandPostings(p Postings) (res []storage.SeriesRef, err error) {
	for p.Next() {
		res = append(res, p.At())
	}
	return res, p.Err()
}

// Postings provides iterative access over a postings list.
// 对倒排索引列表进行遍历的接口
type Postings interface {
	// Next advances the iterator and returns true if another value was found.
	// 迭代器前移，判断是否有元素
	Next() bool

	// Seek advances the iterator to value v or greater and returns
	// true if a value was found.
	// 前移迭代器，查找指定的值（或第一个大于它的值），
	Seek(v storage.SeriesRef) bool

	// At returns the value at the current iterator position.
	// At should only be called after a successful call to Next or Seek.
	// 返回迭代器当前位置的值（SeriesRef），只有当 Next 或 Seek 成功调用之后才能调用 At
	At() storage.SeriesRef

	// Err returns the last error of the iterator.
	Err() error
}

// errPostings is an empty iterator that always errors.
// 实现了 Postings 接口，所有方法都返回 error
type errPostings struct {
	err error
}

func (e errPostings) Next() bool                  { return false }
func (e errPostings) Seek(storage.SeriesRef) bool { return false }
func (e errPostings) At() storage.SeriesRef       { return 0 }
func (e errPostings) Err() error                  { return e.err }

// 表示空的 postings list，调用它的方法会返回 false
var emptyPostings = errPostings{}

// EmptyPostings returns a postings list that's always empty.
// NOTE: Returning EmptyPostings sentinel when Postings struct has no postings is recommended.
// It triggers optimized flow in other functions like Intersect, Without etc.
// 返回一个空的 postings list，可以触发其他函数中的优化策略
func EmptyPostings() Postings {
	return emptyPostings
}

// IsEmptyPostingsType returns true if the postings are an empty postings list.
// When this function returns false, it doesn't mean that the postings isn't empty
// (it could be an empty intersection of two non-empty postings, for example).
// 判断是否为 emptyPostings；false 并不意味着该 postings list 不为空（可能是两个非空postings的交集postings为空）；true 一定为空
func IsEmptyPostingsType(p Postings) bool {
	return p == emptyPostings
}

// ErrPostings returns new postings that immediately error.
// 构造一个 异常的倒排索引列表
func ErrPostings(err error) Postings {
	return errPostings{err}
}

// Intersect returns a new postings list over the intersection of the
// input postings.
// 返回若干输入的交集
func Intersect(its ...Postings) Postings {
	if len(its) == 0 {
		return EmptyPostings()
	}
	if len(its) == 1 {
		return its[0]
	}
	// 确保所有输入都不为空：只要有一个输入为空，结果的交集就是空的
	for _, p := range its {
		if p == EmptyPostings() {
			return EmptyPostings()
		}
	}

	// 构造结果
	return newIntersectPostings(its...)
}

// 一组 postings 的交集，实现了 Postings 接口；寻找交集元素的操作在 Seek() 和 Next() 方法中调用的 doNext() 方法，它用于遍历所有 postings ，在其中寻找共有的元素；
// Seek() 可能会找到比目标值大的第一个共有元素
type intersectPostings struct {
	arr []Postings        // 用于取交集的所有 postings
	cur storage.SeriesRef // 所有 postings 的交集的当前元素 seriesRef
}

// 用多个 Postings 直接构造一个 intersectPostings 结构
func newIntersectPostings(its ...Postings) *intersectPostings {
	return &intersectPostings{arr: its}
}

// At 迭代器当前位置的数据，是所有 postings 的一个共有元素；只能在 Next 或 Seek 成功后调用
func (it *intersectPostings) At() storage.SeriesRef {
	return it.cur
}

// 判断所有 postings 是否存在一个共有元素 it.cur （也可以是大于 it.cur 的元素，会把它设置为新的 cur）
func (it *intersectPostings) doNext() bool {
Loop:
	// 死循环，运行到 return 为止
	for {
		// 遍历所有 postings
		for _, p := range it.arr {
			// 若该 postings 中没有目标 series 或 比目标大的元素，false
			if !p.Seek(it.cur) {
				return false
			}
			// 若找到的只有大于目标的值，设置为新的目标，重新开始遍历所有 postings ，找到新的目标
			if p.At() > it.cur {
				it.cur = p.At() // 设置为暂定的新的交集元素
				continue Loop
			}
		}
		// 在所有 postings 都有目标值，说明该值是交集的一个元素
		return true
	}
}

// Next 前移所有 postings 的迭代器到下一个交集元素的位置，返回值表示是否存在下一个交集元素
func (it *intersectPostings) Next() bool {
	for _, p := range it.arr {
		// 若任一 postings 结束了，表示不会再有交集元素了
		if !p.Next() {
			return false
		}
		// 若当前 postings 中只有比当前共有元素大的值，表示当前共有元素不会是交集元素，指定大的值为新的共有元素
		if p.At() > it.cur {
			it.cur = p.At()
		}
	}
	// 一次遍历结束后，所有的 postings 都进行了 Next()，也设置了整个  interesctPostins 的一个可能的交集元素
	// 接下来需要判断这个可能的交集元素是否存在于所有 postings 中，或者存在大于它的交集元素，记录为 it.cur 作为整个 intersectPostings 的下一个元素
	return it.doNext()
}

// Seek 判断在所有 postings 的交集中是否有指定的 series，或者是大于 id 的 series
func (it *intersectPostings) Seek(id storage.SeriesRef) bool {
	it.cur = id
	return it.doNext()
}

func (it *intersectPostings) Err() error {
	for _, p := range it.arr {
		if p.Err() != nil {
			return p.Err()
		}
	}
	return nil
}

// Merge returns a new iterator over the union of the input iterators.
// 返回一个新的迭代器 Postings，迭代所有输入的 postings 的并集 Union
func Merge(_ context.Context, its ...Postings) Postings {
	if len(its) == 0 {
		return EmptyPostings()
	}
	if len(its) == 1 {
		return its[0]
	}

	p, ok := newMergedPostings(its)
	if !ok {
		return EmptyPostings()
	}
	return p
}

// Loser Tree 败者树是用于多路归并排序（或外部排序）的数据结构，是一种完全二叉树；是胜者树的变体；父结点记录左右结点的败者，让胜者参加下一轮比赛，父结点的父结点再记录这些胜者中的败者
// 败者树中的新元素从叶子结点上升时，只需要读取这条路径中作为败者的父结点，赢了就上升，输了就替换；
// 胜者树需要读取这条路径中的父结点和一部分叶子节点，因为新元素可能替换掉了原本的胜者，现在需要重新比较原本叶子结点中的败者
// 多个 postings 的并集，实现了 Postings 接口
type mergedPostings struct {
	p   []Postings                               // 要取并集的所有 postings
	h   *loser.Tree[storage.SeriesRef, Postings] // 败者树，用 SeriesRef 比较 Postings 的元素
	cur storage.SeriesRef                        // postings 并集的当前元素 seriesRef
}

// 构造 mergedPostings 结构
func newMergedPostings(p []Postings) (m *mergedPostings, nonEmpty bool) {
	// 为 seriesRef 指定一个绝对的最大值，用于败者树
	const maxVal = storage.SeriesRef(math.MaxUint64) // This value must be higher than all real values used in the tree.
	lt := loser.New(p, maxVal)
	// 构造 mergedPostings 结构
	return &mergedPostings{p: p, h: lt}, true
}

// Next 获取 mergedPostings 并集中的下一个元素
func (it *mergedPostings) Next() bool {
	for {
		if !it.h.Next() {
			return false
		}
		// Remove duplicate entries.
		// 获取败者树中的胜者
		newItem := it.h.At()
		// 更新当前的值
		if newItem != it.cur {
			it.cur = newItem
			return true
		}
	}
}

// 寻找 >= id 的 seriesRef
func (it *mergedPostings) Seek(id storage.SeriesRef) bool {
	for !it.h.IsEmpty() && it.h.At() < id {
		finished := !it.h.Winner().Seek(id)
		it.h.Fix(finished) // 调整树结构
	}
	if it.h.IsEmpty() {
		return false
	}
	it.cur = it.h.At()
	return true
}

func (it mergedPostings) At() storage.SeriesRef {
	return it.cur
}

func (it mergedPostings) Err() error {
	for _, p := range it.p {
		if err := p.Err(); err != nil {
			return err
		}
	}
	return nil
}

// Without returns a new postings list that contains all elements from the full list that
// are not in the drop list.
// 两个 Postings 的差集，只在 full 中，不在 drop 中
func Without(full, drop Postings) Postings {
	if full == EmptyPostings() {
		return EmptyPostings()
	}

	if drop == EmptyPostings() {
		return full
	}
	return newRemovedPostings(full, drop)
}

// 取差集的需求导致该 postings 采用了不同于其他的奇怪设计，包括下面三个 bool 变量，以及在 Seek 中调用 Next 的实现方法，这种设计是否有更好的效果，或者说是唯一方案
// 移除 postings 中的部分元素（两个 postings 求差集）
type removedPostings struct {
	full, remove Postings // 全部元素，待移除元素

	cur storage.SeriesRef

	initialized bool
	fok, rok    bool // 对 full 和 remove postings 的 Next 和 Seek 操作是否成功
}

// 构造 removedPostings 结构
func newRemovedPostings(full, remove Postings) *removedPostings {
	return &removedPostings{
		full:   full,
		remove: remove,
	}
}

func (rp *removedPostings) At() storage.SeriesRef {
	return rp.cur
}

// 前移到差集的下一个元素，若 full 和 remove 的元素相等，表示应该移除，直接跳过
func (rp *removedPostings) Next() bool {
	// 若还没有初始化（ removedPostings 创建之后还没调用过 Next 和 Seek），前移两个 postings
	if !rp.initialized {
		rp.fok = rp.full.Next()
		rp.rok = rp.remove.Next()
		rp.initialized = true
	}
	// 死循环，运行到 return 为止
	for {
		// 若 full postings 没有元素
		if !rp.fok {
			return false
		}

		// 若 remove postings 没有元素，直接获取 full 的当前元素，迭代器前移
		if !rp.rok {
			rp.cur = rp.full.At()
			rp.fok = rp.full.Next()
			return true
		}
		// 若两个 postings 都有元素，比较两者
		switch fcur, rcur := rp.full.At(), rp.remove.At(); {
		// 若只存在于 full，返回
		case fcur < rcur:
			rp.cur = fcur
			rp.fok = rp.full.Next()

			return true
			// 若只存在于 remove，继续在 remove 中查找下一个共有元素
		case rcur < fcur:
			// Forward the remove postings to the right position.
			rp.rok = rp.remove.Seek(fcur)
		default:
			// 若两者相等，说明是要移除的元素， full 前移
			// Skip the current posting.
			rp.fok = rp.full.Next()
		}
	}
}

// 分别在两个 postings 中查找，然后用 Next 判断是否需要移除（最终查找结果是 >= id 的 seriesRef）
func (rp *removedPostings) Seek(id storage.SeriesRef) bool {
	// 若当前元素满足要求，直接返回
	if rp.cur >= id {
		return true
	}

	// 分别在两个 postings 中查找
	rp.fok = rp.full.Seek(id)
	rp.rok = rp.remove.Seek(id)
	rp.initialized = true

	return rp.Next()
}

func (rp *removedPostings) Err() error {
	if rp.full.Err() != nil {
		return rp.full.Err()
	}

	return rp.remove.Err()
}

// ListPostings implements the Postings interface over a plain list.
// 在纯列表上实现了 Postings 接口，列表是 seriesRef 数组
type ListPostings struct {
	list []storage.SeriesRef
	cur  storage.SeriesRef // 保存当前值
}

// 构造 ListPostings 结构
func NewListPostings(list []storage.SeriesRef) Postings {
	return newListPostings(list...)
}

// 返回结构指针
func newListPostings(list ...storage.SeriesRef) *ListPostings {
	return &ListPostings{list: list}
}

func (it *ListPostings) At() storage.SeriesRef {
	return it.cur
}

// 迭代器前移，在 it.cur 中保存列表首个元素，并在列表中移除
func (it *ListPostings) Next() bool {
	// 若列表中有元素，获取并移除第一个
	if len(it.list) > 0 {
		it.cur = it.list[0]   // 保存当前值
		it.list = it.list[1:] // 移除
		return true
	}
	it.cur = 0
	return false
}

// 在列表中二分查找 >= id 的元素，移除它和前面的所有元素
func (it *ListPostings) Seek(x storage.SeriesRef) bool {
	// If the current value satisfies, then return.
	// 若当前值满足需求
	if it.cur >= x {
		return true
	}
	if len(it.list) == 0 {
		return false
	}

	// Do binary search between current position and end.
	// 对数组进行二分查找
	i, _ := slices.BinarySearch(it.list, x)
	if i < len(it.list) {
		it.cur = it.list[i]
		it.list = it.list[i+1:]
		return true
	}
	it.list = nil
	return false
}

func (it *ListPostings) Err() error {
	return nil
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
// 在 大端序数字字节流 上实现了 Postings 接口
type bigEndianPostings struct {
	list []byte
	cur  uint32 // 四字节数据
}

// 返回结构指针
func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() storage.SeriesRef {
	return storage.SeriesRef(it.cur) // 转换成 seriesRef 数据
}

// 从字节流中读取并移除 4 byte 数据
func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = binary.BigEndian.Uint32(it.list)
		it.list = it.list[4:]
		return true
	}
	return false
}

// 查找并移除给定数据
func (it *bigEndianPostings) Seek(x storage.SeriesRef) bool {
	// 若当前数据满足需求
	if storage.SeriesRef(it.cur) >= x {
		return true
	}

	// 以四字节为单位进行二分查找
	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= uint32(x)
	})
	// 存入并移除
	if i < num {
		j := i * 4
		it.cur = binary.BigEndian.Uint32(it.list[j:])
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

// FindIntersectingPostings checks the intersection of p and candidates[i] for each i in candidates,
// if intersection is non empty, then i is added to the indexes returned.
// Returned indexes are not sorted.
// 查找输入的 postings 与 p 的交集，返回有交集元素的 candidates index（未排序）
func FindIntersectingPostings(p Postings, candidates []Postings) (indexes []int, err error) {
	// 把 candidates 的所有 postings 及其数组下标存入 小顶堆
	h := make(postingsWithIndexHeap, 0, len(candidates))
	for idx, it := range candidates {
		switch {
		case it.Next():
			h = append(h, postingsWithIndex{index: idx, p: it})
		case it.Err() != nil:
			return nil, it.Err()
		}
	}
	if h.empty() {
		return nil, nil
	}
	heap.Init(&h) // 调整小顶堆结构（初始化）

	// 遍历堆中元素 postings ，寻找交集
	for !h.empty() {
		// 若 p 中所有元素都小于堆中当前最小元素，说明不会有交集了，直接返回
		if !p.Seek(h.at()) {
			return indexes, p.Err()
		}
		// 若找到相等元素了，记录并从堆中移除；否则迭代堆顶元素到下一个值进行比较
		if p.At() == h.at() {
			indexes = append(indexes, h.popIndex())
		} else if err := h.next(); err != nil {
			return nil, err
		}
	}

	return indexes, nil
}

// postingsWithIndex is used as postingsWithIndexHeap elements by FindIntersectingPostings,
// keeping track of the original index of each postings while they move inside the heap.
// 小顶堆的元素
type postingsWithIndex struct {
	index int // 当 postings 在堆中移动时，记录其原本的数组索引
	p     Postings
	// popped means that these postings shouldn't be considered anymore.
	// See popIndex() comment to understand why we need this.
	popped bool // 标记该 postings 是否被从堆中删除（只是标记为删除，并没有真正在堆中移除）
}

// postingsWithIndexHeap implements heap.Interface,
// with root always pointing to the postings with minimum Postings.At() value.
// It also implements a special way of removing elements that marks them as popped and moves them to the bottom of the
// heap instead of actually removing them, see popIndex() for more details.
// 实现了 heap 接口，是一个小顶堆结构，根节点 h[0] 始终指向拥有最小的 Postings.At() 的 postings
// 堆中的元素不会真正被移除，只是会标记为已移除（popped）
// 该堆结构并不会调用 heap.Pop() ，而是使用 popIndex() 把元素标记为 popped 并移动到底部
type postingsWithIndexHeap []postingsWithIndex

// empty checks whether the heap is empty, which is true if it has no elements, of if the smallest element is popped.
// 堆为空或所有元素都标记为移除了
func (h *postingsWithIndexHeap) empty() bool {
	return len(*h) == 0 || (*h)[0].popped
}

// popIndex pops the smallest heap element and returns its index.
// In our implementation we don't actually do heap.Pop(), instead we mark the element as `popped` and fix its position, which
// should be after all the non-popped elements according to our sorting strategy.
// By skipping the `heap.Pop()` call we avoid an extra allocation in this heap's Pop() implementation which returns an interface{}.
// 返回堆顶的最小元素的索引 index，然后重新调整堆结构
func (h *postingsWithIndexHeap) popIndex() int {
	index := (*h)[0].index
	(*h)[0].popped = true
	heap.Fix(h, 0)
	return index
}

// at provides the storage.SeriesRef where root Postings is pointing at this moment.
func (h postingsWithIndexHeap) at() storage.SeriesRef { return h[0].p.At() }

// next performs the Postings.Next() operation on the root of the heap, performing the related operation on the heap
// and conveniently returning the result of calling Postings.Err() if the result of calling Next() was false.
// If Next() succeeds, heap is fixed to move the root to its new position, according to its Postings.At() value.
// If Next() returns fails and there's no error reported by Postings.Err(), then root is marked as removed and heap is fixed.
// 对堆顶最小元素使用 Postings.Next 使其迭代到下一个值，重新调整堆顶元素，返回新的最小元素的索引 postings index
func (h *postingsWithIndexHeap) next() error {
	pi := (*h)[0]
	next := pi.p.Next() // 调用 postings.Next ，移动当前堆顶元素（postings）的迭代器
	if next {           // 若迭代器移动成功，重新调整堆顶元素的位置
		heap.Fix(h, 0)
		return nil
	}

	if err := pi.p.Err(); err != nil {
		return fmt.Errorf("postings %d: %w", pi.index, err)
	}
	h.popIndex() // 获取 重新调整之后的堆顶最小元素
	return nil
}

/*
	以下是实现的 heap 接口的方法
*/

// Len implements heap.Interface.
// Notice that Len() > 0 does not imply that heap is not empty as elements are not removed from this heap.
// Use empty() to check whether heap is empty or not.
func (h postingsWithIndexHeap) Len() int { return len(h) }

// Less implements heap.Interface, it puts all the popped elements at the bottom,
// and then sorts by Postings.At() property of each node.
// 小顶堆的比较方法，把较小的元素上移；较大元素或被移除的元素移到下面
func (h postingsWithIndexHeap) Less(i, j int) bool {
	// 若二者之一被标记为移除了，把被移除的元素移到下面
	if h[i].popped != h[j].popped {
		return h[j].popped
	}
	return h[i].p.At() < h[j].p.At()
}

// Swap implements heap.Interface.
func (h *postingsWithIndexHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// Push implements heap.Interface.
func (h *postingsWithIndexHeap) Push(x interface{}) {
	*h = append(*h, x.(postingsWithIndex))
}

// Pop implements heap.Interface and pops the last element, which is NOT the min element,
// so this doesn't return the same heap.Pop()
// Although this method is implemented for correctness, we don't expect it to be used, see popIndex() method for details.
// 并不是返回堆顶的最小元素，而是返回末尾元素
func (h *postingsWithIndexHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

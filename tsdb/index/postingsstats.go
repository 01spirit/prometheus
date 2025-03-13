// Copyright 2019 The Prometheus Authors
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
	"math"
	"slices"
)

// Stat holds values for a single cardinality statistic.
// 保存单个基数统计的值
type Stat struct {
	Name  string
	Count uint64
}

// 在堆中存储若干统计量，记录最小的元素的值和数组索引；勉强算是个最大堆（并没有实现 container/heap 接口），插入元素靠原地替换，找最小元素靠遍历
type maxHeap struct {
	maxLength int    // 堆的元素最大数量
	minValue  uint64 // 堆中元素的最小值	min([]Stat.Count)
	minIndex  int    // 堆中最小元素的索引
	Items     []Stat // 存储堆中元素
}

// 指定堆的最大长度
func (m *maxHeap) init(length int) {
	m.maxLength = length
	m.minValue = math.MaxUint64
	m.Items = make([]Stat, 0, length)
}

// 把统计量存入 maxHeap，只存入 >= 堆中当前最小元素的, 比较 Stat.Count
func (m *maxHeap) push(item Stat) {
	// 若堆没存满时直接存入元素，更新记录的最小值
	if len(m.Items) < m.maxLength {
		if item.Count < m.minValue {
			m.minValue = item.Count
			m.minIndex = len(m.Items)
		}
		m.Items = append(m.Items, item)
		return
	}
	/* 堆已存满 */
	// 若新元素小于堆中当前最小元素，不存入
	if item.Count < m.minValue {
		return
	}

	/* 新元素 >= 堆中当前最小元素 */
	// 原地替换掉堆中当前最小元素
	m.Items[m.minIndex] = item
	m.minValue = item.Count

	// 遍历堆中所有元素，尝试找到最小的元素（新元素是原地替换的，它未必是堆中的最小元素，需要遍历找到真正的最小元素）
	for i, stat := range m.Items {
		if stat.Count < m.minValue {
			m.minValue = stat.Count
			m.minIndex = i
		}
	}
}

// 把堆中元素( Items []Stat) 按 Stat.Count 从大到小排列后返回
func (m *maxHeap) get() []Stat {
	slices.SortFunc(m.Items, func(a, b Stat) int {
		switch {
		case b.Count < a.Count:
			return -1
		case b.Count > a.Count:
			return 1
		default:
			return 0
		}
	})
	return m.Items
}

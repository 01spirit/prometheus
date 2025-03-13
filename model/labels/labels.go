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

//go:build !stringlabels && !dedupelabels

package labels

import (
	"bytes"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// Labels is a sorted set of labels. Order has to be guaranteed upon
// instantiation.
// 排好序的标签对
type Labels []Label

func (ls Labels) Len() int           { return len(ls) }
func (ls Labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }    // 交换下标对应的标签在数组中的位置
func (ls Labels) Less(i, j int) bool { return ls[i].Name < ls[j].Name } // 用 name 比较两个下标对应的标签

// Bytes returns ls as a byte slice.
// It uses an byte invalid character as a separator and so should not be used for printing.
// 返回 Labels 数组的字节流形式，name 和 value 用特殊分隔符分开
func (ls Labels) Bytes(buf []byte) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(sep)
		}
		b.WriteString(l.Name)
		b.WriteByte(sep)
		b.WriteString(l.Value)
	}
	return b.Bytes()
}

// MatchLabels returns a subset of Labels that matches/does not match with the provided label names based on the 'on' boolean.
// If on is set to true, it returns the subset of labels that match with the provided label names and its inverse when 'on' is set to false.
// 返回所有匹配 names 数组的 Label
func (ls Labels) MatchLabels(on bool, names ...string) Labels {
	matchedLabels := Labels{}

	nameSet := make(map[string]struct{}, len(names))
	for _, n := range names {
		nameSet[n] = struct{}{}
	}

	for _, v := range ls {
		if _, ok := nameSet[v.Name]; on == ok && (on || v.Name != MetricName) {
			matchedLabels = append(matchedLabels, v)
		}
	}

	return matchedLabels
}

// Hash returns a hash value for the label set.
// Note: the result is not guaranteed to be consistent across different runs of Prometheus.
// 返回 Labels 数组的 hash 值
func (ls Labels) Hash() uint64 {
	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	for i, v := range ls {
		if len(b)+len(v.Name)+len(v.Value)+2 >= cap(b) {
			// If labels entry is 1KB+ do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range ls[i:] {
				_, _ = h.WriteString(v.Name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(v.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}

		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

// HashForLabels returns a hash value for the labels matching the provided names.
// 'names' have to be sorted in ascending order.
// 返回指定标签的 hash 值，names 升序排列
func (ls Labels) HashForLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	i, j := 0, 0
	for i < len(ls) && j < len(names) {
		switch {
		case names[j] < ls[i].Name:
			j++
		case ls[i].Name < names[j]:
			i++
		default:
			b = append(b, ls[i].Name...)
			b = append(b, sep)
			b = append(b, ls[i].Value...)
			b = append(b, sep)
			i++
			j++
		}
	}
	return xxhash.Sum64(b), b
}

// HashWithoutLabels returns a hash value for all labels except those matching
// the provided names.
// 'names' have to be sorted in ascending order.
// 返回指定标签以外的 hash 值，names 升序排列
func (ls Labels) HashWithoutLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	for i := range ls {
		for j < len(names) && names[j] < ls[i].Name {
			j++
		}
		if ls[i].Name == MetricName || (j < len(names) && ls[i].Name == names[j]) {
			continue
		}
		b = append(b, ls[i].Name...)
		b = append(b, sep)
		b = append(b, ls[i].Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b), b
}

// BytesWithLabels is just as Bytes(), but only for labels matching names.
// 'names' have to be sorted in ascending order.
// 返回指定的标签的字节流，names 升序排列
func (ls Labels) BytesWithLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	i, j := 0, 0
	for i < len(ls) && j < len(names) {
		switch {
		case names[j] < ls[i].Name:
			j++
		case ls[i].Name < names[j]:
			i++
		default:
			if b.Len() > 1 {
				b.WriteByte(sep)
			}
			b.WriteString(ls[i].Name)
			b.WriteByte(sep)
			b.WriteString(ls[i].Value)
			i++
			j++
		}
	}
	return b.Bytes()
}

// BytesWithoutLabels is just as Bytes(), but only for labels not matching names.
// 'names' have to be sorted in ascending order.
// 返回指定标签以外的字节流，names 升序
func (ls Labels) BytesWithoutLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	j := 0
	for i := range ls {
		for j < len(names) && names[j] < ls[i].Name {
			j++
		}
		if j < len(names) && ls[i].Name == names[j] {
			continue
		}
		if b.Len() > 1 {
			b.WriteByte(sep)
		}
		b.WriteString(ls[i].Name)
		b.WriteByte(sep)
		b.WriteString(ls[i].Value)
	}
	return b.Bytes()
}

// Copy returns a copy of the labels.
// 返回切片的复制
func (ls Labels) Copy() Labels {
	res := make(Labels, len(ls))
	copy(res, ls)
	return res
}

// Get returns the value for the label with the given name.
// Returns an empty string if the label doesn't exist.
// Get name->value	""
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Has returns true if the label with the given name is present.
// 是否存在该 name
func (ls Labels) Has(name string) bool {
	for _, l := range ls {
		if l.Name == name {
			return true
		}
	}
	return false
}

// HasDuplicateLabelNames returns whether ls has duplicate label names.
// It assumes that the labelset is sorted.
// 是否有重复的 name，Labels 需要有序
func (ls Labels) HasDuplicateLabelNames() (string, bool) {
	for i, l := range ls {
		if i == 0 {
			continue
		}
		if l.Name == ls[i-1].Name {
			return l.Name, true
		}
	}
	return "", false
}

// WithoutEmpty returns the labelset without empty labels.
// May return the same labelset.
// 返回 value 都不为空的子集
func (ls Labels) WithoutEmpty() Labels {
	for _, v := range ls {
		if v.Value != "" {
			continue
		}
		// Do not copy the slice until it's necessary.
		// 因为不一定存在符合条件的标签，如果在函数开始就创建切片，可能直到结束都用不到，浪费资源；直到需要切片时才创建，然后从当前位置继续循环，返回结果，外层循环不再使用
		els := make(Labels, 0, len(ls)-1) // 直到有空值时才分配，从这里开始遍历然后退出
		for _, v := range ls {
			if v.Value != "" {
				els = append(els, v)
			}
		}
		return els
	}
	return ls
}

// Equal returns whether the two label sets are equal.
// 遍历比较所有标签是否相等
func Equal(ls, o Labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i, l := range ls {
		if l != o[i] {
			return false
		}
	}
	return true
}

// EmptyLabels returns n empty Labels value, for convenience.	返回空 Labels 结构
func EmptyLabels() Labels {
	return Labels{}
}

// New returns a sorted Labels from the given labels.
// The caller has to guarantee that all label names are unique.
// 返回按字典序升序排列的 Label 数组，输入保证 Label 是唯一的
func New(ls ...Label) Labels {
	set := make(Labels, 0, len(ls)) // 预先声明容量为 len(ls)
	set = append(set, ls...)
	slices.SortFunc(set, func(a, b Label) int { return strings.Compare(a.Name, b.Name) }) // 按字典序排列

	return set
}

// FromStrings creates new labels from pairs of strings.
// 用输入的多个 string 创建 Labels 并排序
func FromStrings(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	res := make(Labels, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		res = append(res, Label{Name: ss[i], Value: ss[i+1]})
	}

	slices.SortFunc(res, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	return res
}

// Compare compares the two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
// 找到更大的 Label：比较两个 Labels 的 name 和 value，最后比较长度，a < b 返回 -1， a > b 返回 1
func Compare(a, b Labels) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if a[i].Name != b[i].Name {
			if a[i].Name < b[i].Name {
				return -1
			}
			return 1
		}
		if a[i].Value != b[i].Value {
			if a[i].Value < b[i].Value {
				return -1
			}
			return 1
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

// CopyFrom copies labels from b on top of whatever was in ls previously,
// reusing memory or expanding if needed.
// 用给定标签覆盖原有的标签
func (ls *Labels) CopyFrom(b Labels) {
	(*ls) = append((*ls)[:0], b...)
}

// IsEmpty returns true if ls represents an empty set of labels.
// 长度为零
func (ls Labels) IsEmpty() bool {
	return len(ls) == 0
}

// Range calls f on each label.
// 遍历 Labels 数组的所有 Label，对其调用函数 f()
func (ls Labels) Range(f func(l Label)) {
	for _, l := range ls {
		f(l)
	}
}

// Validate calls f on each label. If f returns a non-nil error, then it returns that error cancelling the iteration.
// 遍历对每个 Label 调用函数 f()，进行异常处理
func (ls Labels) Validate(f func(l Label) error) error {
	for _, l := range ls {
		if err := f(l); err != nil {
			return err
		}
	}
	return nil
}

// DropMetricName returns Labels with "__name__" removed.
// 删除 name == "__name__" 的标签
func (ls Labels) DropMetricName() Labels {
	for i, l := range ls {
		if l.Name == MetricName {
			if i == 0 { // Make common case fast with no allocations.
				return ls[1:]
			}
			// Avoid modifying original Labels - use [:i:i] so that left slice would not
			// have any spare capacity and append would have to allocate a new slice for the result.
			return append(ls[:i:i], ls[i+1:]...)
		}
	}
	return ls
}

// InternStrings calls intern on every string value inside ls, replacing them with what it returns.
func (ls *Labels) InternStrings(intern func(string) string) {
	for i, l := range *ls {
		(*ls)[i].Name = intern(l.Name)
		(*ls)[i].Value = intern(l.Value)
	}
}

// ReleaseStrings calls release on every string value inside ls.
func (ls Labels) ReleaseStrings(release func(string)) {
	for _, l := range ls {
		release(l.Name)
		release(l.Value)
	}
}

// Builder allows modifying Labels.		用于修改 Labels 数组
type Builder struct {
	base Labels
	del  []string
	add  []Label
}

// Reset clears all current state for the builder. 清理 value 为空的标签，把 name 存入 del 数组
func (b *Builder) Reset(base Labels) {
	b.base = base
	b.del = b.del[:0]
	b.add = b.add[:0]
	b.base.Range(func(l Label) {
		if l.Value == "" {
			b.del = append(b.del, l.Name) // 遍历，把所有空标签存入 del 数组
		}
	})
}

// Labels returns the labels from the builder.
// If no modifications were made, the original labels are returned.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 { // 无修改，返回原始数组
		return b.base
	}

	expectedSize := len(b.base) + len(b.add) - len(b.del) // 结果数组的预期大小
	if expectedSize < 1 {
		expectedSize = 1
	}
	res := make(Labels, 0, expectedSize)
	for _, l := range b.base {
		if slices.Contains(b.del, l.Name) || contains(b.add, l.Name) { // 跳过修改过的标签
			continue
		}
		res = append(res, l) // 存入结果数组
	}
	if len(b.add) > 0 { // Base is already in order, so we only need to sort if we add to it.	// 把添加进来的标签加入结果数组，排序
		res = append(res, b.add...)
		slices.SortFunc(res, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	}
	return res
}

// ScratchBuilder allows efficient construction of a Labels from scratch.
// 允许从头开始高效构建标签
type ScratchBuilder struct {
	add Labels
}

// SymbolTable is no-op, just for api parity with dedupelabels.	无操作，仅为保持 api 一致性
type SymbolTable struct{}

func NewSymbolTable() *SymbolTable { return nil }

func (t *SymbolTable) Len() int { return 0 }

// NewScratchBuilder creates a ScratchBuilder initialized for Labels with n entries.
func NewScratchBuilder(n int) ScratchBuilder {
	return ScratchBuilder{add: make([]Label, 0, n)}
}

// NewBuilderWithSymbolTable creates a Builder, for api parity with dedupelabels.
func NewBuilderWithSymbolTable(_ *SymbolTable) *Builder {
	return NewBuilder(EmptyLabels())
}

// NewScratchBuilderWithSymbolTable creates a ScratchBuilder, for api parity with dedupelabels.
func NewScratchBuilderWithSymbolTable(_ *SymbolTable, n int) ScratchBuilder {
	return NewScratchBuilder(n)
}

func (b *ScratchBuilder) SetSymbolTable(_ *SymbolTable) {
	// no-op
}

func (b *ScratchBuilder) Reset() {
	b.add = b.add[:0]
}

// Add a name/value pair.
// Note if you Add the same name twice you will get a duplicate label, which is invalid.
func (b *ScratchBuilder) Add(name, value string) {
	b.add = append(b.add, Label{Name: name, Value: value})
}

// UnsafeAddBytes adds a name/value pair, using []byte instead of string.
// The '-tags stringlabels' version of this function is unsafe, hence the name.
// This version is safe - it copies the strings immediately - but we keep the same name so everything compiles.
// 以字节流的形式传入标签对
func (b *ScratchBuilder) UnsafeAddBytes(name, value []byte) {
	b.add = append(b.add, Label{Name: string(name), Value: string(value)})
}

// Sort the labels added so far by name.
func (b *ScratchBuilder) Sort() {
	slices.SortFunc(b.add, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
}

// Assign is for when you already have a Labels which you want this ScratchBuilder to return.
// 用给定的 Labels 数组覆盖原有的 Labels
func (b *ScratchBuilder) Assign(ls Labels) {
	b.add = append(b.add[:0], ls...) // Copy on top of our slice, so we don't retain the input slice.
}

// Labels returns the name/value pairs added so far as a Labels object.
// Note: if you want them sorted, call Sort() first.
// 从 builder 中获取 Labels 数组，未排序
func (b *ScratchBuilder) Labels() Labels {
	// Copy the slice, so the next use of ScratchBuilder doesn't overwrite.
	return append([]Label{}, b.add...)
}

// Overwrite the newly-built Labels out to ls.
// Callers must ensure that there are no other references to ls, or any strings fetched from it.
// 把给定的 Labels 数组用 ScratchBuilder 覆盖
func (b *ScratchBuilder) Overwrite(ls *Labels) {
	*ls = append((*ls)[:0], b.add...)
}

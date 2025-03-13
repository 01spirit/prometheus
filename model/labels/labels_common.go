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

package labels

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"unsafe"

	"github.com/prometheus/common/model"
)

const (
	MetricName   = "__name__"
	AlertName    = "alertname"
	BucketLabel  = "le"
	InstanceName = "instance"

	// 用两个不常用字符作为 分隔符
	labelSep = '\xfe' // Used at beginning of `Bytes` return.	在所有标签之前
	sep      = '\xff' // Used between labels in `Bytes` and `Hash`.	在标签之间
)

var seps = []byte{sep} // Used with Hash, which has no WriteByte method.

// Label is a key/value pair of strings.
type Label struct {
	Name, Value string
}

// 把所有 Label 排列成一个 string 	{firstLabel="first", secondLabel="second"}
func (ls Labels) String() string {
	var bytea [1024]byte            // On stack to avoid memory allocation while building the output.	在栈上声明变量，避免动态内存分配，减少内存分配的开销和垃圾回收压力；动态内存分配通过 new 和 make 关键字实现堆内存的分配
	b := bytes.NewBuffer(bytea[:0]) // 使用 bytea 的内存空间，但是不包含任何数据

	b.WriteByte('{')
	i := 0
	ls.Range(func(l Label) { // 遍历 Labels 数组，对每个 Label 调用自定义函数
		if i > 0 { // 在第一个 Label 之后加上 ", "
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.Write(strconv.AppendQuote(b.AvailableBuffer(), l.Value)) // 申请 buffer 中的可用容量，写入其中；给 Value 加上双引号
		i++
	})
	b.WriteByte('}')
	return b.String()
}

// MarshalJSON implements json.Marshaler.
func (ls Labels) MarshalJSON() ([]byte, error) {
	return json.Marshal(ls.Map()) // 序列化为 json 格式的字节流
}

// UnmarshalJSON implements json.Unmarshaler.
func (ls *Labels) UnmarshalJSON(b []byte) error {
	var m map[string]string

	if err := json.Unmarshal(b, &m); err != nil { // 从字节流中反序列化出 Labels map
		return err
	}

	*ls = FromMap(m) // 用 map 构造顺序的 Label 数组
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (ls Labels) MarshalYAML() (interface{}, error) {
	return ls.Map(), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (ls *Labels) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var m map[string]string

	if err := unmarshal(&m); err != nil {
		return err
	}

	*ls = FromMap(m)
	return nil
}

// IsValid checks if the metric name or label names are valid.	验证字符合法性
func (ls Labels) IsValid(validationScheme model.ValidationScheme) bool {
	err := ls.Validate(func(l Label) error { // 遍历 Labels，调用自定义函数
		if l.Name == model.MetricNameLabel {
			// If the default validation scheme has been overridden with legacy mode,
			// we need to call the special legacy validation checker.
			if validationScheme == model.LegacyValidation && model.NameValidationScheme == model.UTF8Validation && !model.IsValidLegacyMetricName(string(model.LabelValue(l.Value))) {
				return strconv.ErrSyntax
			}
			if !model.IsValidMetricName(model.LabelValue(l.Value)) {
				return strconv.ErrSyntax
			}
		}
		if validationScheme == model.LegacyValidation && model.NameValidationScheme == model.UTF8Validation {
			if !model.LabelName(l.Name).IsValidLegacy() || !model.LabelValue(l.Value).IsValid() {
				return strconv.ErrSyntax
			}
		} else if !model.LabelName(l.Name).IsValid() || !model.LabelValue(l.Value).IsValid() {
			return strconv.ErrSyntax
		}
		return nil
	})
	return err == nil
}

// Map returns a string map of the labels.	把 Labels 数组转换成 map[name]value
func (ls Labels) Map() map[string]string {
	m := make(map[string]string)
	ls.Range(func(l Label) {
		m[l.Name] = l.Value
	})
	return m
}

// FromMap returns new sorted Labels from the given map.	把 Label map 转换成排好序的数组 Labels
func FromMap(m map[string]string) Labels {
	l := make([]Label, 0, len(m))
	for k, v := range m {
		l = append(l, Label{Name: k, Value: v})
	}
	return New(l...)
}

// NewBuilder returns a new LabelsBuilder.
func NewBuilder(base Labels) *Builder {
	b := &Builder{
		del: make([]string, 0, 5),
		add: make([]Label, 0, 5),
	}
	b.Reset(base)
	return b
}

// Del deletes the label of the given name.
func (b *Builder) Del(ns ...string) *Builder {
	for _, n := range ns {
		for i, a := range b.add {
			if a.Name == n { // 从 add 数组中去掉待删除标签
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n) // 加入 del 数组
	}
	return b
}

// Keep removes all labels from the base except those with the given names.	只保留所需标签，其余删除
func (b *Builder) Keep(ns ...string) *Builder {
	b.base.Range(func(l Label) {
		for _, n := range ns {
			if l.Name == n {
				return
			}
		}
		b.del = append(b.del, l.Name)
	})
	return b
}

// Set the name/value pair as a label. A value of "" means delete that label.
func (b *Builder) Set(n, v string) *Builder {
	if v == "" { // value 置为空等同于删除该标签
		// Empty labels are the same as missing labels.
		return b.Del(n)
	}
	for i, a := range b.add {
		if a.Name == n { // 覆盖修改记录
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, Label{Name: n, Value: v}) // 添加记录

	return b
}

func (b *Builder) Get(n string) string {
	// Del() removes entries from .add but Set() does not remove from .del, so check .add first.
	for _, a := range b.add {
		if a.Name == n {
			return a.Value
		}
	}
	if slices.Contains(b.del, n) {
		return ""
	}
	return b.base.Get(n)
}

// Range calls f on each label in the Builder. 遍历 builder 中可用的标签，先是 base，再是 add
func (b *Builder) Range(f func(l Label)) {
	// Stack-based arrays to avoid heap allocation in most cases.
	var addStack [128]Label
	var delStack [128]string
	// Take a copy of add and del, so they are unaffected by calls to Set() or Del().
	origAdd, origDel := append(addStack[:0], b.add...), append(delStack[:0], b.del...)
	b.base.Range(func(l Label) {
		if !slices.Contains(origDel, l.Name) && !contains(origAdd, l.Name) {
			f(l)
		}
	})
	for _, a := range origAdd {
		f(a)
	}
}

// 遍历数组，查找 Label.Name == n，
func contains(s []Label, n string) bool {
	for _, a := range s {
		if a.Name == n {
			return true
		}
	}
	return false
}

// 把字节流转换成 string
func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b))) // 取地址，转换成 Pointer（可以存储任何指针的类型），转换成 *string (string 指针)，解引用
}

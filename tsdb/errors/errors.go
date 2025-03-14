// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errors

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// multiError type allows combining multiple errors into one.	允许一次返回多种错误
type multiError []error

// NewMulti returns multiError with provided errors added if not nil.	把传入的 errors 整合成一个结构
func NewMulti(errs ...error) multiError { //nolint:revive // unexported-return.
	m := multiError{}
	m.Add(errs...)
	return m
}

// Add adds single or many errors to the error list. Each error is added only if not nil.
// If the error is a nonNilMultiError type, the errors inside nonNilMultiError are added to the main multiError.
// 向 error list 中添加不为 nil 的 error；若 error 是 nonNilMultiError 类型的，把其中的所有 errors 存入 multiError
// nonNilMultiError 实现了 error 接口，参数列表中可以传入该类型
func (es *multiError) Add(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}
		var merr nonNilMultiError
		if errors.As(err, &merr) {
			*es = append(*es, merr.errs...) // 若 err 是 nonNilMultiError 类型的，存入其中所有 errors
			continue
		}
		*es = append(*es, err) // 把 err 存入
	}
}

// Err returns the error list as an error or nil if it is empty.
// 把 multiError 结构作为单个 error 返回，为空时返回 nil
func (es multiError) Err() error {
	if len(es) == 0 {
		return nil
	}
	return nonNilMultiError{errs: es}
}

// nonNilMultiError implements the error interface, and it represents
// multiError with at least one error inside it.
// This type is needed to make sure that nil is returned when no error is combined in multiError for err != nil
// check to work.
// 实现了 error 接口，表示 multiError 中至少有一个 error，可以作为 error 类型返回，把 multiError 包装成一个 error
type nonNilMultiError struct {
	errs multiError
}

// Error returns a concatenated string of the contained errors.
// 实现 error 接口的方法，异常信息拼接成 string
func (es nonNilMultiError) Error() string {
	var buf bytes.Buffer

	if len(es.errs) > 1 {
		fmt.Fprintf(&buf, "%d errors: ", len(es.errs))
	}

	for i, err := range es.errs {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}

	return buf.String()
}

// Is attempts to match the provided error against errors in the error list.
//
// This function allows errors.Is to traverse the values stored in the MultiError.
// It returns true if any of the errors in the list match the target.
// 在 error list 中遍历匹配相应的 error
func (es nonNilMultiError) Is(target error) bool {
	for _, err := range es.errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// CloseAll closes all given closers while recording error in MultiError.
// 关闭所有 io.Closer，把 error 存入 multiError，返回为单个 error （nonNilMultiError）
func CloseAll(cs []io.Closer) error {
	errs := NewMulti()
	for _, c := range cs {
		errs.Add(c.Close())
	}
	return errs.Err()
}

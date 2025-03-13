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

package timestamp

import (
	"math"
	"time"
)

// FromTime returns a new millisecond timestamp from a time.
// Time 类型转换为 int64 时间戳，单位 毫秒 ms
func FromTime(t time.Time) int64 {
	return t.Unix()*1000 + int64(t.Nanosecond())/int64(time.Millisecond)
}

// Time returns a new time.Time object from a millisecond timestamp.
// int64 时间戳 （单位 ms） 转化为 Time 类型
func Time(ts int64) time.Time {
	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond)).UTC()
}

// FromFloatSeconds returns a millisecond timestamp from float seconds.
// 把浮点型的 单位 秒 的时间戳转换为 int64 毫秒 时间戳
func FromFloatSeconds(ts float64) int64 {
	return int64(math.Round(ts * 1000))
}

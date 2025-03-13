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

package exemplar

import "github.com/prometheus/prometheus/model/labels"

// ExemplarMaxLabelSetLength is defined by OpenMetrics: "The combined length of
// the label names and values of an Exemplar's LabelSet MUST NOT exceed 128
// UTF-8 characters."
// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
const ExemplarMaxLabelSetLength = 128

// Exemplar is additional information associated with a time series.
// Exemplar 是和时间序列相关联的额外信息
type Exemplar struct {
	Labels labels.Labels `json:"labels"`    // 标签对的数组
	Value  float64       `json:"value"`     // 时间序列的 值
	Ts     int64         `json:"timestamp"` // 时间戳
	HasTs  bool          // 是否拥有时间戳
}

type QueryResult struct {
	SeriesLabels labels.Labels `json:"seriesLabels"`
	Exemplars    []Exemplar    `json:"exemplars"`
}

// Equals compares if the exemplar e is the same as e2. Note that if HasTs is false for
// both exemplars then the timestamps will be ignored for the comparison. This can come up
// when an exemplar is exported without it's own timestamp, in which case the scrape timestamp
// is assigned to the Ts field. However we still want to treat the same exemplar, scraped without
// an exported timestamp, as a duplicate of itself for each subsequent scrape.
// 比较两个 Exemplar 是否相等，比较顺序： Labels -> HasTs -> Ts -> Value
func (e Exemplar) Equals(e2 Exemplar) bool {
	if !labels.Equal(e.Labels, e2.Labels) {
		return false
	}

	if (e.HasTs || e2.HasTs) && e.Ts != e2.Ts {
		return false
	}

	return e.Value == e2.Value
}

// Compare first timestamps, then values, then labels.
// 比较两个 Exemplar，a > b -> 1, a < b -> -1		比较顺序：Ts -> Value -> labels
func Compare(a, b Exemplar) int {
	if a.Ts < b.Ts {
		return -1
	} else if a.Ts > b.Ts {
		return 1
	}
	if a.Value < b.Value {
		return -1
	} else if a.Value > b.Value {
		return 1
	}
	return labels.Compare(a.Labels, b.Labels)
}

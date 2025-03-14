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

package metadata

import "github.com/prometheus/common/model"

// Metadata stores a series' metadata information.
// 时间序列的元数据信息
type Metadata struct {
	Type model.MetricType `json:"type"` // 指标类型（仪表读数 Gauge， 直方图 Histogram 等）
	Unit string           `json:"unit"`
	Help string           `json:"help"`
}

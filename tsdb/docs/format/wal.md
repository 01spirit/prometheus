# WAL Disk Format

预写日志组织为序号连续的段，每个段默认限制为128MB。一个段以32KB的页面写入，只有最新段的最后一个页面可以是不完整的。
一个WAL record是不透明的字节数组，如果超过了当前页面的剩余空间，会被划分为子记录（sub-records）。
记录不会跨越段的边界分割。如果单个记录超过了段的默认大小，会创建更大的段。

The write ahead log operates in segments that are numbered and sequential,
e.g. `000000`, `000001`, `000002`, etc., and are limited to 128MB by default.
A segment is written to in pages of 32KB. Only the last page of the most recent segment
may be partial. A WAL record is an opaque byte slice that gets split up into sub-records
should it exceed the remaining space of the current page. Records are never split across
segment boundaries. If a single record exceeds the default segment size, a segment with
a larger size will be created.
The encoding of pages is largely borrowed from [LevelDB's/RocksDB's write ahead log.](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)

Notable deviations are that the record fragment is encoded as:

```
┌───────────┬──────────┬────────────┬──────────────┐
│ type <1b> │ len <2b> │ CRC32 <4b> │ data <bytes> │
└───────────┴──────────┴────────────┴──────────────┘
```

The type flag has the following states:

* `0`: rest of page will be empty
* `1`: a full record encoded in a single fragment
* `2`: first fragment of a record
* `3`: middle fragment of a record
* `4`: final fragment of a record

## Record encoding

The records written to the write ahead log are encoded as follows:

### Series records

Series records encode the labels that identifies a series and its unique ID.

```
┌────────────────────────────────────────────┐
│ type = 1 <1b>                              │
├────────────────────────────────────────────┤
│ ┌─────────┬──────────────────────────────┐ │
│ │ id <8b> │ n = len(labels) <uvarint>    │ │
│ ├─────────┴────────────┬─────────────────┤ │
│ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
│ ├──────────────────────┴─────────────────┤ │
│ │  ...                                   │ │
│ ├───────────────────────┬────────────────┤ │
│ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │
│ └───────────────────────┴────────────────┘ │
│                  . . .                     │
└────────────────────────────────────────────┘
```

### Sample records

Sample records encode samples as a list of triples `(series_id, timestamp, value)`.
Series reference and timestamp are encoded as deltas w.r.t the first sample.
The first row stores the starting id and the starting timestamp.
The first sample record begins at the second row.

```
┌──────────────────────────────────────────────────────────────────┐
│ type = 2 <1b>                                                    │
├──────────────────────────────────────────────────────────────────┤
│ ┌────────────────────┬───────────────────────────┐               │
│ │ id <8b>            │ timestamp <8b>            │               │
│ └────────────────────┴───────────────────────────┘               │
│ ┌────────────────────┬───────────────────────────┬─────────────┐ │
│ │ id_delta <uvarint> │ timestamp_delta <uvarint> │ value <8b>  │ │
│ └────────────────────┴───────────────────────────┴─────────────┘ │
│                              . . .                               │
└──────────────────────────────────────────────────────────────────┘
```

### Tombstone records

Tombstone records encode tombstones as a list of triples `(series_id, min_time, max_time)`
and specify an interval for which samples of a series got deleted.

```
┌─────────────────────────────────────────────────────┐
│ type = 3 <1b>                                       │
├─────────────────────────────────────────────────────┤
│ ┌─────────┬───────────────────┬───────────────────┐ │
│ │ id <8b> │ min_time <varint> │ max_time <varint> │ │
│ └─────────┴───────────────────┴───────────────────┘ │
│                        . . .                        │
└─────────────────────────────────────────────────────┘
```

### Exemplar records

Exemplar records encode exemplars as a list of triples `(series_id, timestamp, value)` 
plus the length of the labels list, and all the labels.
The first row stores the starting id and the starting timestamp.
Series reference and timestamp are encoded as deltas w.r.t the first exemplar.
The first exemplar record begins at the second row.

See: https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars

```
┌──────────────────────────────────────────────────────────────────┐
│ type = 4 <1b>                                                    │
├──────────────────────────────────────────────────────────────────┤
│ ┌────────────────────┬───────────────────────────┐               │
│ │ id <8b>            │ timestamp <8b>            │               │
│ └────────────────────┴───────────────────────────┘               │
│ ┌────────────────────┬───────────────────────────┬─────────────┐ │
│ │ id_delta <uvarint> │ timestamp_delta <uvarint> │ value <8b>  │ │
│ ├────────────────────┴───────────────────────────┴─────────────┤ │
│ │  n = len(labels) <uvarint>                                   │ │
│ ├──────────────────────┬───────────────────────────────────────┤ │
│ │ len(str_1) <uvarint> │ str_1 <bytes>                         │ │
│ ├──────────────────────┴───────────────────────────────────────┤ │
│ │  ...                                                         │ │
│ ├───────────────────────┬──────────────────────────────────────┤ │
│ │ len(str_2n) <uvarint> │ str_2n <bytes> │                     │ │
│ └───────────────────────┴────────────────┴─────────────────────┘ │
│                              . . .                               │
└──────────────────────────────────────────────────────────────────┘
```

### Metadata records

Metadata records encode the metadata updates associated with a series.

```
┌────────────────────────────────────────────┐
│ type = 6 <1b>                              │
├────────────────────────────────────────────┤
│ ┌────────────────────────────────────────┐ │
│ │ series_id <uvarint>                    │ │
│ ├────────────────────────────────────────┤ │
│ │ metric_type <1b>                       │ │
│ ├────────────────────────────────────────┤ │
│ │ num_fields <uvarint>                   │ │
│ ├───────────────────────┬────────────────┤ │
│ │ len(name_1) <uvarint> │ name_1 <bytes> │ │
│ ├───────────────────────┼────────────────┤ │
│ │ len(val_1) <uvarint>  │ val_1 <bytes>  │ │
│ ├───────────────────────┴────────────────┤ │
│ │                . . .                   │ │
│ ├───────────────────────┬────────────────┤ │
│ │ len(name_n) <uvarint> │ name_n <bytes> │ │
│ ├───────────────────────┼────────────────┤ │
│ │ len(val_n) <uvarint>  │ val_n <bytes>  │ │
│ └───────────────────────┴────────────────┘ │
│                  . . .                     │
└────────────────────────────────────────────┘
```


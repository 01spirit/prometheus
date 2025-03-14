# Index Disk Format

每个block中有一个 index 文件，以 TOC （文件内容目录）结尾，TOC 是 index 文件的入口

The following describes the format of the `index` file found in each block directory.
It is terminated by a table of contents which serves as an entry point into the index.

```
┌────────────────────────────┬─────────────────────┐
│ magic(0xBAAAD700) <4b>     │ version(1) <1 byte> │
├────────────────────────────┴─────────────────────┤
│ ┌──────────────────────────────────────────────┐ │
│ │                 Symbol Table                 │ │
│ ├──────────────────────────────────────────────┤ │
│ │                    Series                    │ │
│ ├──────────────────────────────────────────────┤ │
│ │                 Label Index 1                │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      ...                     │ │
│ ├──────────────────────────────────────────────┤ │
│ │                 Label Index N                │ │
│ ├──────────────────────────────────────────────┤ │
│ │                   Postings 1                 │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      ...                     │ │
│ ├──────────────────────────────────────────────┤ │
│ │                   Postings N                 │ │
│ ├──────────────────────────────────────────────┤ │
│ │               Label Offset Table             │ │
│ ├──────────────────────────────────────────────┤ │
│ │             Postings Offset Table            │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      TOC                     │ │
│ └──────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
```

When the index is written, an arbitrary number of padding bytes may be added between the lined out main sections above. When sequentially scanning through the file, any zero bytes after a section's specified length must be skipped.

Most of the sections described below start with a `len` field. It always specifies the number of bytes just before the trailing CRC32 checksum. The checksum is always calculated over those `len` bytes.


### Symbol Table

**符号表：** 有序的字符串列表，包含了在存储的时间序列Series的标签对中出现过的字符串（去重），标签对字符串会被随后的部分引用（Reference）\
符号表中包含了一系列字符串条目，起始是字符串的原始长度，String编码是utf-8，通过顺序索引引用，按字典序升序排列


The symbol table holds a sorted list of deduplicated strings that occur in label pairs of the stored series. They can be referenced from subsequent sections and significantly reduce the total index size.

The section contains a sequence of the string entries, each prefixed with the string's length in raw bytes. All strings are utf-8 encoded.
Strings are referenced by sequential indexing. The strings are sorted in lexicographically ascending order.

```
┌────────────────────┬─────────────────────┐
│ len <4b>           │ #symbols <4b>       │
├────────────────────┴─────────────────────┤
│ ┌──────────────────────┬───────────────┐ │
│ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
│ ├──────────────────────┴───────────────┤ │
│ │                . . .                 │ │
│ ├──────────────────────┬───────────────┤ │
│ │ len(str_n) <uvarint> │ str_n <bytes> │ │
│ └──────────────────────┴───────────────┘ │
├──────────────────────────────────────────┤
│ CRC32 <4b>                               │
└──────────────────────────────────────────┘
```


### Series

**序列：** 这部分包含了序列Series，其中包含了序列的标签集和序列在block中所在的chunks。序列按照标签集的字典序排列\
每个Series section对齐为16字节，序列ID是 offset/16，作为后续索引reference的series ID。\
每个Series条目首先是标签数量，接着是包含标签名和标签值的字符表Symbol table的引用的元组。\
标签之后是indexed chunks的数量，接着是元数据条目，包含了chunks的最小和最大时间戳，以及该chunk在chunk file中的位置的索引。\
单个序列内的chunk索引必须是递增的，不同序列间的chunk索引也必须是递增的。这一特性确保了属于同一序列的chunks在segment file中被放在一起。\
一个序列的所有chunk的mint和maxt不能重叠。\



The section contains a sequence of series that hold the label set of the series as well as its chunks within the block. The series are sorted lexicographically by their label sets.  
Each series section is aligned to 16 bytes. The ID for a series is the `offset/16`. This serves as the series' ID in all subsequent references. Thereby, a sorted list of series IDs implies a lexicographically sorted list of series label sets. 

```
┌───────────────────────────────────────┐
│ ┌───────────────────────────────────┐ │
│ │   series_1                        │ │
│ ├───────────────────────────────────┤ │
│ │                 . . .             │ │
│ ├───────────────────────────────────┤ │
│ │   series_n                        │ │
│ └───────────────────────────────────┘ │
└───────────────────────────────────────┘
```

Every series entry first holds its number of labels, followed by tuples of symbol table references that contain the label name and value. The label pairs are lexicographically sorted.  
After the labels, the number of indexed chunks is encoded, followed by a sequence of metadata entries containing the chunks minimum (`mint`) and maximum (`maxt`) timestamp and a reference to its position in the chunk file. The `mint` is the time of the first sample and `maxt` is the time of the last sample in the chunk. Holding the time range data in the index allows dropping chunks irrelevant to queried time ranges without accessing them directly.

Chunk references within single series must be increasing, and chunk references for `series_(N+1)` must be higher than chunk references for `series_N`.
This property guarantees that chunks that belong to the same series are grouped together in the segment files.
Furthermore chunk `mint` must be less or equal than `maxt`, and subsequent chunks within single series must have increasing `mint` and `maxt` and not overlap.

`mint` of the first chunk is stored, it's `maxt` is stored as a delta and the `mint` and `maxt` are encoded as deltas to the previous time for subsequent chunks. Similarly, the reference of the first chunk is stored and the next ref is stored as a delta to the previous one.

```
┌──────────────────────────────────────────────────────────────────────────┐
│ len <uvarint>                                                            │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌──────────────────────────────────────────────────────────────────────┐ │
│ │                     labels count <uvarint64>                         │ │
│ ├──────────────────────────────────────────────────────────────────────┤ │
│ │              ┌────────────────────────────────────────────┐          │ │
│ │              │ ref(l_i.name) <uvarint32>                  │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ ref(l_i.value) <uvarint32>                 │          │ │
│ │              └────────────────────────────────────────────┘          │ │
│ │                             ...                                      │ │
│ ├──────────────────────────────────────────────────────────────────────┤ │
│ │                     chunks count <uvarint64>                         │ │
│ ├──────────────────────────────────────────────────────────────────────┤ │
│ │              ┌────────────────────────────────────────────┐          │ │
│ │              │ c_0.mint <varint64>                        │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ ref(c_0.data) <uvarint64>                  │          │ │
│ │              └────────────────────────────────────────────┘          │ │
│ │              ┌────────────────────────────────────────────┐          │ │
│ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
│ │              └────────────────────────────────────────────┘          │ │
│ │                             ...                                      │ │
│ └──────────────────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────┤
│ CRC32 <4b>                                                               │
└──────────────────────────────────────────────────────────────────────────┘
```



### Label Index

**标签索引：** 索引一或多个标签名的现存的值

A label index section indexes the existing (combined) values for one or more label names.
The `#names` field determines the number of indexed label names, followed by the total number of entries in the `#entries` field. The body holds `#entries / #names` tuples of symbol table references, each tuple being of #names length. The value tuples are sorted in lexicographically increasing order. **This is no longer used.**

```
┌───────────────┬────────────────┬────────────────┐
│ len <4b>      │ #names <4b>    │ #entries <4b>  │
├───────────────┴────────────────┴────────────────┤
│ ┌─────────────────────────────────────────────┐ │
│ │ ref(value_0) <4b>                           │ │
│ ├─────────────────────────────────────────────┤ │
│ │ ...                                         │ │
│ ├─────────────────────────────────────────────┤ │
│ │ ref(value_n) <4b>                           │ │
│ └─────────────────────────────────────────────┘ │
│                      . . .                      │
├─────────────────────────────────────────────────┤
│ CRC32 <4b>                                      │
└─────────────────────────────────────────────────┘
```

For instance, a single label name with 4 different values will be encoded as:

```
┌────┬───┬───┬──────────────┬──────────────┬──────────────┬──────────────┬───────┐
│ 24 │ 1 │ 4 │ ref(value_0) | ref(value_1) | ref(value_2) | ref(value_3) | CRC32 |
└────┴───┴───┴──────────────┴──────────────┴──────────────┴──────────────┴───────┘
```

The sequence of label index sections is finalized by a [label offset table](#label-offset-table) containing label offset entries that points to the beginning of each label index section for a given label name.

### Postings
Posting 就是指 Series ID
**倒排索引：** 存储与给定标签对相关联的Series reference的单调递增列表。\
section 末尾是倒排索引偏移表，表中包含了给定标签对的倒排索引的起始地址偏移量。

Postings sections store monotonically increasing lists of series references that contain a given label pair associated with the list.

```
┌────────────────────┬────────────────────┐
│ len <4b>           │ #entries <4b>      │
├────────────────────┴────────────────────┤
│ ┌─────────────────────────────────────┐ │
│ │ ref(series_1) <4b>                  │ │
│ ├─────────────────────────────────────┤ │
│ │ ...                                 │ │
│ ├─────────────────────────────────────┤ │
│ │ ref(series_n) <4b>                  │ │
│ └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│ CRC32 <4b>                              │
└─────────────────────────────────────────┘
```

The sequence of postings sections is finalized by a [postings offset table](#postings-offset-table) containing postings offset entries that points to the beginning of each postings section for a given label pair.

### Label Offset Table

A label offset table stores a sequence of label offset entries.
Every label offset entry holds the label name and the offset to its values in the label index section.
They are used to track label index sections. **This is no longer used.**

```
┌─────────────────────┬──────────────────────┐
│ len <4b>            │ #entries <4b>        │
├─────────────────────┴──────────────────────┤
│ ┌────────────────────────────────────────┐ │
│ │  n = 1 <1b>                            │ │
│ ├──────────────────────┬─────────────────┤ │
│ │ len(name) <uvarint>  │ name <bytes>    │ │
│ ├──────────────────────┴─────────────────┤ │
│ │  offset <uvarint64>                    │ │
│ └────────────────────────────────────────┘ │
│                    . . .                   │
├────────────────────────────────────────────┤
│  CRC32 <4b>                                │
└────────────────────────────────────────────┘
```


### Postings Offset Table

**倒排索引偏移表：** 存储一系列倒排索引偏移量条目，按标签名和值排序。\
当index文件被加载时，读取该表的一部分到内存中。

A postings offset table stores a sequence of postings offset entries, sorted by label name and value.
Every postings offset entry holds the label name/value pair and the offset to its series list in the postings section.
They are used to track postings sections. They are partially read into memory when an index file is loaded.

```
┌─────────────────────┬──────────────────────┐
│ len <4b>            │ #entries <4b>        │
├─────────────────────┴──────────────────────┤
│ ┌────────────────────────────────────────┐ │
│ │  n = 2 <1b>                            │ │
│ ├──────────────────────┬─────────────────┤ │
│ │ len(name) <uvarint>  │ name <bytes>    │ │
│ ├──────────────────────┼─────────────────┤ │
│ │ len(value) <uvarint> │ value <bytes>   │ │
│ ├──────────────────────┴─────────────────┤ │
│ │  offset <uvarint64>                    │ │
│ └────────────────────────────────────────┘ │
│                    . . .                   │
├────────────────────────────────────────────┤
│  CRC32 <4b>                                │
└────────────────────────────────────────────┘
```


### TOC

**目录：** 目录作为整个index文件的入口，指向文件中的各个区域，如果reference是0，表明相应的section不存在，应该在查找时返回空结果。

The table of contents serves as an entry point to the entire index and points to various sections in the file.
If a reference is zero, it indicates the respective section does not exist and empty results should be returned upon lookup.

```
┌─────────────────────────────────────────┐
│ ref(symbols) <8b>                       │
├─────────────────────────────────────────┤
│ ref(series) <8b>                        │
├─────────────────────────────────────────┤
│ ref(label indices start) <8b>           │
├─────────────────────────────────────────┤
│ ref(label offset table) <8b>            │
├─────────────────────────────────────────┤
│ ref(postings start) <8b>                │
├─────────────────────────────────────────┤
│ ref(postings offset table) <8b>         │
├─────────────────────────────────────────┤
│ CRC32 <4b>                              │
└─────────────────────────────────────────┘
```

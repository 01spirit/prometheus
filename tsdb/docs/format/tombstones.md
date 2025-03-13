# Tombstones Disk Format

The following describes the format of a tombstones file, which is placed
at the top level directory of a block.

The last 8 bytes specifies the offset to the start of Stones section.
The stones section is 0 padded to a multiple of 4 for fast scans.

```
┌────────────────────────────┬─────────────────────┐
│ magic(0x0130BA30) <4b>     │ version(1) <1 byte> │
├────────────────────────────┴─────────────────────┤
│ ┌──────────────────────────────────────────────┐ │
│ │                Tombstone 1                   │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      ...                     │ │
│ ├──────────────────────────────────────────────┤ │
│ │                Tombstone N                   │ │
│ ├──────────────────────────────────────────────┤ │
│ │                  CRC<4b>                     │ │
│ └──────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
```

最后8字节指向Stone区域的起始地址，整个区域用0填充为4的倍数

# Tombstone 

```
┌───────────────────────┬─────────────────┬────────────────┐
│series ref <uvarint64> │ mint <varint64> │ maxt <varint64>│
└───────────────────────┴─────────────────┴────────────────┘
```

删除指定时间序列的时间区间内的数据\
**Tombstones：**\
tombstones文件用于实现数据的软删除，不会立即从物理存储中删除时间序列数据，而是在tombstone文件中记录删除标记，在后续的压缩和合并操作中，安全地移除这些被标记的数据，同时确保数据的完整性和一致性
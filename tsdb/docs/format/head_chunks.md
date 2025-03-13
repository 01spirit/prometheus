# Head Chunks on Disk Format

The following describes the format of a chunks file, which is created in the
`chunks_head/` directory inside the data directory.

Chunks in the files are referenced from the index by uint64 composed of
in-file offset (lower 4 bytes) and segment sequence number (upper 4 bytes).

```
┌──────────────────────────────┐
│  magic(0x0130BC91) <4 byte>  │
├──────────────────────────────┤
│    version(1) <1 byte>       │
├──────────────────────────────┤
│    padding(0) <3 byte>       │
├──────────────────────────────┤
│ ┌──────────────────────────┐ │
│ │         Chunk 1          │ │
│ ├──────────────────────────┤ │
│ │          ...             │ │
│ ├──────────────────────────┤ │
│ │         Chunk N          │ │
│ └──────────────────────────┘ │
└──────────────────────────────┘
```


# Chunk

Unlike chunks in the on-disk blocks, here we additionally store the series
reference that each chunk belongs to and the mint/maxt of the chunks. This is
because we don't have an index associated with these chunks, hence this metadata
is used while replaying the chunks.

```
┌─────────────────────┬───────────────────────┬───────────────────────┬───────────────────┬───────────────┬──────────────┬────────────────┐
| series ref <8 byte> | mint <8 byte, uint64> | maxt <8 byte, uint64> | encoding <1 byte> | len <uvarint> | data <bytes> │ CRC32 <4 byte> │
└─────────────────────┴───────────────────────┴───────────────────────┴───────────────────┴───────────────┴──────────────┴────────────────┘
```


head chunk用于处理最近写入的数据。数据写入时会首先进入内存中的head chunk。head chunk随着新数据写入而增长，达到一定大小或时间范围之后，被冻结并写入磁盘，同时创建一个新的head chunk接收新数据\
head chunk中的数据在内存中，便于快速访问；把写满的head chunk写入磁盘并用内存映射（mmap）技术；数据写入磁盘可以被压缩
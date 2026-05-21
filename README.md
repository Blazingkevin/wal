# wal

A write-ahead log library for Go. Built as the durability primitive for a chain of
infrastructure projects.

> **Status: actively in development.** The record encoding layer is complete and
> benchmarked. The segment writer, recovery reader, and public API are being built next.
> This README will reflect the current state of the implementation.

---

## Why

Every serious storage system starts with a
write-ahead log. The WAL is what makes a committed write survive a power failure or
process crash. Without it, any multi-step operation that spans more than one disk write
is vulnerable to partial failure: some writes succeed, some don't, and the data is left
in an inconsistent state with no way to recover.

The WAL solves this with one rule: **record your intent before you act.** Write a log
entry describing the mutation, flush it to disk, then apply the mutation. If the process
crashes after the log write but before the data write, the log entry is replayed on
restart. The mutation is reapplied. Nothing is lost.

This library is built from first principles, the record format, checksum strategy,
segment rotation, and sync policies are all implemented directly against the Go standard
library with zero external dependencies.

---

## What Is Built

### `internal/record` — Binary Record Encoding

The foundation layer. Encodes arbitrary byte payloads into the on-disk WAL record
format and decodes them back, with CRC32c integrity checking on every record.

**On-disk layout:**

```
[length: uint32, 4 bytes][checksum: uint32, 4 bytes][data: N bytes]
```

The checksum covers both the length field and the data payload. This matters: a torn
write that corrupts only the length field would cause a naive decoder to read the wrong
number of bytes and return garbage with a passing checksum. Covering the length field
catches this immediately.

**Benchmarks** (Apple M4, Go 1.24.2):

```
BenchmarkEncode   27 ns/op   4,655 MB/s   144 B/op   1 allocs/op
BenchmarkDecode   10 ns/op  12,950 MB/s     0 B/op   0 allocs/op
```

`Decode` has zero allocations — it returns a sub-slice of the input buffer. In a
recovery path reading millions of records, this matters.

## Design Decisions

A full write-up of design decisions and the reasoning behind them will live in
`ARCHITECTURE.md` once the implementation is complete. Decisions made so far:

**CRC32c over CRC32 (IEEE).**
Go's `hash/crc32` package uses the CLMUL hardware instruction on AMD64 and ARM64 for
the Castagnoli polynomial, making checksumming effectively free on modern hardware. The
IEEE polynomial does not have the same hardware path in Go's standard library.

**Checksum covers the length field.**
A torn write can corrupt the length field specifically. If only the payload were
checksummed, a corrupt length would cause the decoder to read the wrong number of bytes
and return garbage data with a passing checksum, silent corruption. Covering both
catches this case immediately.

**Zero external dependencies.**
This library uses only the Go standard library. Every project that imports it inherits
its dependency footprint. For a low-level primitive meant to be embedded in storage
systems, adding third-party dependencies is the wrong trade-off.

---

## References

- [The Log: What Every Software Engineer Should Know](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — Jay Kreps
- [ARIES: A Transaction Recovery Method](https://dl.acm.org/doi/10.1145/128765.128770) — Mohan et al., 1992
- [Files Are Hard](https://danluu.com/file-consistency/) — Dan Luu
- [SQLite WAL Mode](https://www.sqlite.org/wal.html)

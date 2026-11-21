# Phase 3 — analytics engine microbenchmark

**Scope:** in-process `MemoryEngine.OnEvent` only (no Kafka, no PostgreSQL).  
**Machine:** Apple M5, darwin/arm64, Go 1.23.  
**Command:** `go test ./stream-processor/internal/analytics -bench=BenchmarkOnEvent -benchmem -count=1`

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkOnEvent` (default specs: 1s + 1m tumbling + 5m/1m sliding) | 2602 | 2049 | 56 |
| `BenchmarkOnEventTumblingOnly` (1s + 1m) | 839 | 695 | 17 |

These numbers measure **windowing overhead**, not end-to-end ingest throughput.
Do not advertise them as system capacity.

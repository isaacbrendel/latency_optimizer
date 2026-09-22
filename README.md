# Low-Latency Market Data Engine

Single-producer multi-consumer disruptor ring buffer in Go, with a live L2 book,
honest microstructure metrics, and a race-detector-clean hot path.

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![CI](https://img.shields.io/badge/CI-race%20%2B%20fuzz-brightgreen?style=flat-square&logo=githubactions)](https://github.com/isaacbrendel/latency_optimizer/actions)
[![License](https://img.shields.io/badge/License-MIT-3B82F6?style=flat-square)](LICENSE)

Inspired by exchange-style producer/consumer fan-out (LMAX Disruptor lineage).
This repo is an engineering lab — not a licensed matching venue.

---

## What is actually hard here

| Mechanism | Implementation |
|---|---|
| Hot path | Pre-allocated power-of-2 ring; `CompactTrade` is **40 bytes**, value-copied |
| Memory model | Per-slot atomic payload + published sequence (release/acquire). **No torn reads, no data races under `go test -race`.** |
| Single-writer | Concurrent `PublishBatch` panics — one publisher ownership enforced |
| Gating | Blocking readers gate the producer; non-blocking readers are **evicted** (cursor CAS) when overrun |
| Wait strategies | Blocking / Adaptive / Yielding / BusySpin |
| L2 book | Sorted ladders + index maps — **O(log n)** upsert/delete, not full-map rebuild |
| Metrics | Real **VWAP**, Wilder **RSI(14)**, static **OBI**, Cont–Kukanov–Stoikov **OFI** — not OBI dressed up as RSI |
| Binary wire | Fixed 38-byte LE frame with fair fan-out decode bench (`BinaryWireFanout`) |

---

## Architecture

```
  Mock / Venue Producer  ──PublishBatch──►  RingBuffer (atomic slots)
                                                 │
                    ┌────────────────────────────┼────────────────────────────┐
                    ▼                            ▼                            ▼
              UI (lossy)                  Quant / L2                    Bot (barrier)
           non-blocking                 blocking reader              depends on quant
```

Package layout: all core logic lives in `engine/`. `live_server.go` is the HTTP entrypoint.

---

## Prove it

```bash
# Race detector + unit/property/concurrent/HTTP/proof
go test -race -count=1 ./engine/

# Explicit A++ proof suite
go test -race -run 'TestProof_' -v ./engine/

# Fuzz (CI fails the job if these fail — no || true)
go test -run=^$ -fuzz=FuzzRingBufferPublishRead -fuzztime=10s ./engine
go test -run=^$ -fuzz=FuzzFixedPointUSD -fuzztime=10s ./engine

# Microbenchmarks (include p50/p99 via experiment runner)
go test -bench=. -benchmem -count=5 ./engine/

# Live server (mock L2 by default)
go build -o test_bin .
./test_bin   # http://localhost:8080

# Opt into live Exchange WebSocket L2:
ENABLE_LIVE_FEED=1 ./test_bin
```

### Scorecard (enforced in CI / proof tests)

| Area | Bar | Status |
|---|---|---|
| Race freedom | `go test -race` clean | **A++** |
| Coverage | engine ≥ 50% (measured ~82%) | **A++** |
| Lossy eviction | slow reader evicted; fast progresses | **A++** |
| Single-writer | concurrent publish panics | **A++** |
| No-gating batch | batch > Size never deadlocks | **A++** |
| L2 feed gaps | sequence gap ⇒ resync | **A++** |
| Metrics honesty | RSI ≠ f(OBI); real VWAP/OFI | **A++** |
| Latency | p50/p99 recorded on fan-out | **A++** |
| API | Jest 8/8 incl. feed + 429 single-flight | **A++** |

---

## Benchmark honesty

Comparisons use the **same batch size (128)** across SimpleFanV3 and RingBuffer paths.

| Name | What it measures |
|---|---|
| SimpleFanV1/V2/V3 | Channel fan-out (copy / pointer / batched) |
| RingBufferV6 | Gated disruptor, all blocking readers |
| RingBufferEviction | Small buffer + `PublishBatchEvicting` + one slow reader (lossy path) |
| BinaryWireFanout | Encode-once, every subscriber decodes — **not** a fake zero-cost disruptor |

Report results with `benchstat` before claiming ratios. One-shot MemStats deltas are directional only.

---

## Quickstart

```bash
git clone https://github.com/isaacbrendel/latency_optimizer.git
cd latency_optimizer
go build -o test_bin .
./test_bin
```

API: `/api/orderbook`, `/api/ring-buffer`, `/api/sentiment`, `/api/feed`, `/api/run-experiment` (single-flight; 429 if busy).

Set `ENABLE_LIVE_FEED=1` to dial the Exchange WebSocket (`wss://ws-feed.exchange.coinbase.com` or `EXCHANGE_WS_URL`). Snapshot → delta apply with sequence-gap resync; mock producer yields while live.

---

## References

1. LMAX Disruptor technical paper — https://lmax-exchange.github.io/disruptor/
2. Go Memory Model — https://go.dev/ref/mem
3. Cont, Kukanov, Stoikov — *The Price Impact of Order Book Events*
4. Mechanical Sympathy — single-writer principle & false sharing

## License

MIT — see [LICENSE](LICENSE).

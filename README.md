# Low-Latency Market Data Engine

Single-producer multi-consumer disruptor ring buffer in Go, with a live Coinbase BTC-USD L2 book,
honest microstructure metrics, and a race-detector-clean hot path.

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![CI](https://img.shields.io/badge/CI-race%20%2B%20fuzz%20%2B%20Jest-brightgreen?style=flat-square&logo=githubactions)](https://github.com/isaacbrendel/latency_optimizer/actions)
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
| Binary wire | Fixed 38-byte LE frame (`flatbuffers_trade.go` — hand-rolled LE, not FlatBuffers IDL) with fair fan-out decode bench (`BinaryWireFanout`) |
| Live feed | Coinbase Exchange **BTC-USD** only — WebSocket L2 on long-running hosts; REST snapshots on serverless |

---

## Architecture

```
  Coinbase L2 (WS or REST) / Mock  ──PublishBatch──►  RingBuffer (atomic slots)
                                                           │
           ┌───────────────┬───────────────┬───────────────┼───────────────┐
           ▼               ▼               ▼               ▼
        UI (lossy)     Quant / IND      Bot (barrier)    Audit (lossy)
      non-blocking     blocking         depends on       non-blocking
                                       quant reader
```

### Layout

| Path | Role |
|---|---|
| `engine/` | Ring buffer, L2 book, feed (WS + REST), microstructure, HTTP handlers, proof/bench tests |
| `live_server.go` | Long-running HTTP entrypoint (`/` → `docs/`, `/api/*`) |
| `api/*/` | Vercel Go serverless wrappers → same `engine` handlers |
| `docs/` | Live dashboard UI (order book, ring slots, feed telemetry, charts) |
| `tests/` | Jest API integration suite (starts `server_test_bin` with mock feed) |
| `.github/workflows/` | CI (race + coverage floor + short fuzz + Jest) and nightly soak |

---

## Prove it

```bash
# Race detector + unit/property/concurrent/HTTP/proof
go test -race -count=1 ./engine/

# Explicit A++ proof suite
go test -race -run 'TestProof_' -v ./engine/

# Fuzz — CI uses 5s; Makefile / local default below; nightly uses 30s
go test -run=^$ -fuzz=FuzzRingBufferPublishRead -fuzztime=10s ./engine
go test -run=^$ -fuzz=FuzzFixedPointUSD -fuzztime=10s ./engine

# Microbenchmarks (include p50/p99 via experiment runner)
go test -bench=. -benchmem -count=5 ./engine/

# Or via Makefile
make test
make fuzz
make bench

# Live server — real Coinbase BTC-USD L2 is ON by default
make run
#   → http://localhost:8080  (static UI from docs/)
#   long-running: WebSocket L2 (wss://ws-feed.exchange.coinbase.com) → mode=live
#   serverless (Vercel): REST L2 snapshots (~1.5s TTL / on each API request) → mode=live_rest

# Force mock (CI / Jest / offline):
DISABLE_LIVE_FEED=1 ./test_bin

# Force WebSocket even on Vercel (usually not useful — connections are ephemeral):
ENABLE_LIVE_FEED=1

# Jest API contract tests (needs server_test_bin first)
go build -o server_test_bin .
npm ci && npm test
```

### Scorecard (enforced in CI / proof tests)

| Area | Bar | Status |
|---|---|---|
| Race freedom | `go test -race` clean | **A++** |
| Coverage | engine ≥ 50% (measured ~80%) | **A++** |
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
make run
# open http://localhost:8080
```

### HTTP API

| Endpoint | Notes |
|---|---|
| `GET /api/orderbook` | Live L2 top-of-book + depth |
| `GET /api/ring-buffer` | Slot inspection + consumer cursors |
| `GET /api/sentiment` | VWAP / RSI / OBI / OFI snapshot |
| `GET /api/feed` | Feed telemetry (`status`, `mode`, gaps, sequences) |
| `GET /api/run-experiment` | SSE benchmark stream; **single-flight** (429 if busy) |

### Live market data

Unless `DISABLE_LIVE_FEED=1`:

- **Long-running host:** Exchange WebSocket L2 (default on; `ENABLE_LIVE_FEED=1` forces WS even on serverless) with snapshot → delta + sequence-gap resync (`mode=live`).
- **Serverless (Vercel):** public REST L2 snapshots (top 50), refreshed every ~1.5s and on each API request (`mode=live_rest`) — real BTC-USD prices without a persistent socket.
- **Overrides:** `EXCHANGE_WS_URL`, `EXCHANGE_REST_URL` for custom endpoints.

Feed status values: `disconnected` · `connecting` · `live` · `live_rest` · `resyncing` · `fallback_mock`.

Deployed on Vercel via `vercel.json` rewrites (`/api/*` → Go functions under `api/`, everything else → `docs/`).

---

## References

1. LMAX Disruptor technical paper — https://lmax-exchange.github.io/disruptor/
2. Go Memory Model — https://go.dev/ref/mem
3. Cont, Kukanov, Stoikov — *The Price Impact of Order Book Events*
4. Mechanical Sympathy — single-writer principle & false sharing

## License

MIT — see [LICENSE](LICENSE).

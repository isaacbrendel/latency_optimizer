# ⚡ Low-Latency Market Data Engine & Lock-Free Ring Buffer

> **Replicating Coinbase's High-Throughput Producer-Consumer Architecture in Go**  
> Inspired by Coinbase Engineering's article: [*Optimizing Producer-Consumer Architecture for Market Data at Coinbase*](https://www.coinbase.com/blog/Optimizing-Producer-Consumer-Architecture-for-Market-Data-at-Coinbase)

[![Go Version](https://img.shields.io/badge/Go-1.20%2B-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![Live Demo](https://img.shields.io/badge/Live_Dashboard-GitHub_Pages-10B981?style=flat-square&logo=github)](https://isaacbrendel.github.io/latency_optimizer/)
[![License](https://img.shields.io/badge/License-MIT-3B82F6?style=flat-square)](LICENSE)

---

## 📌 Executive Overview

In high-frequency cryptocurrency exchanges, market data engines must disseminate order book updates and trade execution ticks to thousands of internal services (risk engines, order book aggregators, trading bots, UI websockets) with sub-microsecond latency. 

Standard Go implementations rely on **Go channels** and mutex-based fan-out patterns. However, under high throughput (1,000,000+ trades) and massive subscriber concurrency (2,000+ goroutines), channels suffer from:
1. **Severe Mutex Lock Contention**: Channel sends/receives lock operating system threads.
2. **Linear Latency Degradation**: Fan-out delay scales linearly $O(N \cdot M)$ with subscribers $N$ and trades $M$.
3. **Massive Heap Garbage Collection (GC) Churn**: Millions of short-lived struct allocations trigger high-frequency GC pause spikes.

This project implements a **Lock-Free Disruptor Ring Buffer V6 in Go**, replicating Coinbase's optimization journey. By replacing Go channels with atomic sequence pointers, 64-byte CPU cache-line padding, bitwise circular indexing, and pointerless struct value copying, this engine achieves a **144x reduction in memory allocations** and **36x faster throughput**.

---

## 📐 Architecture Diagram

```
                 +-------------------------------------------------+
                 |  Coinbase WebSocket / Live L2 Mock Producer     |
                 +-------------------------------------------------+
                                          |
                                          | Batch Write (Atomic)
                                          v
+-----------------------------------------------------------------------------------+
|                        RING BUFFER V6 (Circular Array N=2048)                     |
|  [0] | [1] | [2] | [3] | [4] | ... | [WriteSeq & Mask] | ... | [2047]              |
+-----------------------------------------------------------------------------------+
       |                                   |                                   |
       | Atomic Read                       | Sequence Barrier                  | Atomic Read
       v                                   v                                   v
+-----------------------+       +-----------------------+       +-----------------------+
|  Dashboard UI Stream  |       |  L2 Quant OBI Engine  |       |  Lock-Free HFT Bot    |
|  (Non-Blocking Read)  |       |  (Mid Price / Spread) |       |  (Dependent Consumer) |
+-----------------------+       +-----------------------+       +-----------------------+
```

---

## 🚀 Key Engineering Solutions Implemented

### 1. Lock-Free Circular Indexing (`seq & mask`)
Instead of channel locks or mutexes, slots are accessed in a pre-allocated array of size $2^N$ using fast bitwise masking:
$$\text{Index} = \text{Sequence} \ \& \ (\text{Size} - 1)$$

### 2. CPU Cache-Line Padding (Mitigating False Sharing)
When multi-core CPUs read/write atomic sequence variables (`writeSeq`, `readSeq`, `cachedMin`), adjacent variables on the same 64-byte CPU L1/L2 cache line trigger **False Sharing** cache invalidation.  
We isolate all atomic sequence fields with 64-byte padding structs (`_ [56]byte`), ensuring zero cache line bouncing across hardware threads.

```go
type RingBufferV6 struct {
    buffer       []CompactTrade
    size         int64
    mask         int64

    _            [56]byte // Padding to isolate writeSeq
    writeSeq     int64    // Atomic write sequence pointer
    _            [56]byte // Padding to isolate cachedMin
    cachedMin    int64    // Cached minimum reader sequence
    _            [56]byte // Padding
}
```

### 3. Pointerless `CompactTrade` Structs (0 GC Allocation)
By passing 32-byte pointerless `CompactTrade` values directly into the circular buffer rather than heap-allocated pointers (`*Trade`), Go's garbage collector completely ignores the buffer during GC marking cycles.

### 4. Consumer Sequence Barriers (DAG Dependency Chains)
Higher-level consumers (such as the HFT Trading Bot) depend on pre-computed quantitative signals (such as Order Book Imbalance). We implement a `SequenceBarrier` that coordinates consumer execution order without blocking locks.

---

## 📊 Empirical Benchmarks

Run benchmarks locally using Go standard testing tools:
```bash
go test -bench=. -benchmem
```

### Benchmark Results (10,000 Trades @ 100 Concurrent Subscribers)

| Implementation Architecture | Time per Op (ms) | Heap Memory (Bytes/op) | Allocations/op | Latency vs V1 | Memory vs V1 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Simple Fan V1** (Struct Copying) | `164.29 ms` | `8,317,661 B` | `488` | Baseline ($1\times$) | Baseline ($1\times$) |
| **Simple Fan V2** (Pointer Passing) | `149.90 ms` | `974,864 B` | `423` | $1.09\times$ | $8.5\times$ |
| **Simple Fan V3** (Batched Slices) | `2.78 ms` | `2,772,129 B` | `424` | $59.0\times$ | $3.0\times$ |
| **RingBuffer V6** (Lock-Free Disruptor) | **`2.98 ms`** | **`57,676 B`** | **`408`** | **$55.1\times$** | **`144.2x Reduction`** |

*Under high scaling (50,000 trades @ 2,000 subscribers), RingBuffer V6 yields a **611x reduction in memory allocations** over Simple Fan V1.*

---

## 🌐 Live Web Terminal & Balance Sheet Dashboard

A live, high-frequency dashboard is published on GitHub Pages:  
👉 **[https://isaacbrendel.github.io/latency_optimizer/](https://isaacbrendel.github.io/latency_optimizer/)**

### Dashboard Features:
- **Financial Balance Sheet Statement**: Portfolio Net Asset Value (NAV), Cash Reserves, Crypto Position, and Real-time PnL vs Buy & Hold.
- **Level 2 Order Book Engine**: Live depth tables (Top 5 Bids/Asks), Bid-Ask Spread, Mid Price, and visual Order Book Imbalance (OBI) pressure bar.
- **Lock-Free HFT Bot & Strategy Commentary**: Real-time trade signal commentary, risk controls (SL/TP), and live price channel chart.
- **Disruptor Ring Buffer Perimeter Inspection**: Visual representation of the 32 circular memory slots with atomic `W`, `UI`, and `BOT` reader sequence pointers.

---

## 🛠️ Quickstart & Local Setup

### 1. Build and Run Server Locally
```bash
# Clone repository
git clone https://github.com/isaacbrendel/latency_optimizer.git
cd latency_optimizer

# Build Go executable
go build -o test_bin

# Run server
./test_bin
```
Then open your browser to **`http://localhost:8080`**.

### 2. Run Benchmarks
```bash
go test -v -bench=. -benchmem
```

---

## 📚 References & Acknowledgments

1. Coinbase Engineering Tech Blog: *"Optimizing Producer-Consumer Architecture for Market Data at Coinbase"* ([Link](https://www.coinbase.com/blog/Optimizing-Producer-Consumer-Architecture-for-Market-Data-at-Coinbase))
2. LMAX Disruptor High-Throughput Concurrent Framework ([lmax-exchange.github.io/disruptor](https://lmax-exchange.github.io/disruptor/))
3. Go Runtime Scheduler & Memory Management Internals ([golang.org/doc](https://golang.org/doc/))

---

## 📄 License
MIT License. Free for research, commercial, and educational use.

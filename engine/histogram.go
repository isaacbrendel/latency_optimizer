package engine

import (
	"math"
	"sort"
	"sync"
)

// LatencyHistogram records nanosecond samples and reports percentiles.
// Lock-free for single recorder; mutex for merge/snapshot from tests.
type LatencyHistogram struct {
	mu      sync.Mutex
	samples []int64
	maxN    int
}

// NewLatencyHistogram keeps up to maxN samples (ring truncate oldest).
func NewLatencyHistogram(maxN int) *LatencyHistogram {
	if maxN < 64 {
		maxN = 64
	}
	return &LatencyHistogram{samples: make([]int64, 0, maxN), maxN: maxN}
}

// Record appends a latency sample in nanoseconds.
func (h *LatencyHistogram) Record(ns int64) {
	if ns < 0 {
		ns = 0
	}
	h.mu.Lock()
	if len(h.samples) >= h.maxN {
		// drop oldest half to bound memory
		h.samples = append(h.samples[:0], h.samples[len(h.samples)/2:]...)
	}
	h.samples = append(h.samples, ns)
	h.mu.Unlock()
}

// Percentiles returns p50/p90/p99/p999 and count. Values in nanoseconds.
func (h *LatencyHistogram) Percentiles() (p50, p90, p99, p999 int64, n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n = len(h.samples)
	if n == 0 {
		return 0, 0, 0, 0, 0
	}
	cp := append([]int64(nil), h.samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return pct(cp, 50), pct(cp, 90), pct(cp, 99), pct(cp, 99.9), n
}

func pct(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// LatencyReport is JSON-serializable percentile output.
type LatencyReport struct {
	P50Ns  int64 `json:"p50Ns"`
	P90Ns  int64 `json:"p90Ns"`
	P99Ns  int64 `json:"p99Ns"`
	P999Ns int64 `json:"p999Ns"`
	Count  int   `json:"count"`
}

func (h *LatencyHistogram) Report() LatencyReport {
	p50, p90, p99, p999, n := h.Percentiles()
	return LatencyReport{P50Ns: p50, P90Ns: p90, P99Ns: p99, P999Ns: p999, Count: n}
}

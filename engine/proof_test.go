package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// A++ proof suite: explicit contracts that must hold for production-grade claims.

func TestProof_CompactTradeWireSize(t *testing.T) {
	TestCompactTradeSize(t)
}

func TestProof_BlockingConsumersZeroLossUnderRace(t *testing.T) {
	TestRingBuffer_NoLossBlockingConsumers(t)
}

func TestProof_SingleWriterPanic(t *testing.T) {
	TestRingBuffer_SingleWriterEnforced(t)
}

func TestProof_EvictionDoesNotStallPublisher(t *testing.T) {
	TestRingBuffer_RaceFreeEviction(t)
}

func TestProof_OrderBookLogN(t *testing.T) {
	TestOrderBook_LogNUpdatesUnderLoad(t)
}

func TestProof_MetricsAreHonest(t *testing.T) {
	TestMicrostructure_VWAPAndOFI(t)
	TestMicrostructure_RSINotDisguisedOBI(t)
}

func TestProof_FeedGapResync(t *testing.T) {
	TestL2Feed_SequenceGapTriggersResync(t)
}

func TestProof_HDRPercentilesMonotonic(t *testing.T) {
	h := NewLatencyHistogram(10_000)
	for i := int64(0); i < 1000; i++ {
		h.Record((i + 1) * 100)
	}
	p50, p90, p99, p999, n := h.Percentiles()
	if n != 1000 {
		t.Fatalf("n=%d", n)
	}
	if !(p50 <= p90 && p90 <= p99 && p99 <= p999) {
		t.Fatalf("percentiles not monotonic: %d %d %d %d", p50, p90, p99, p999)
	}
}

func TestProof_RingBufferLatencyP99Bound(t *testing.T) {
	const n = 2000
	rb := NewRingBufferV6(1024, 1)
	hist := NewLatencyHistogram(n)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rb.Read(rb.Readers[0], n, nil, func(ct CompactTrade) {
			if ct.Timestamp > 0 {
				hist.Record(time.Now().UnixNano() - ct.Timestamp)
			}
		})
	}()
	batch := make([]CompactTrade, 64)
	for i := 0; i < n; i += 64 {
		ts := time.Now().UnixNano()
		for j := 0; j < 64; j++ {
			batch[j] = CompactTrade{ID: int64(i + j + 1), Timestamp: ts, Price: USD(i + j)}
		}
		rb.PublishBatch(batch)
	}
	wg.Wait()
	rep := hist.Report()
	if rep.Count == 0 {
		t.Fatal("expected latency samples")
	}
	// Local process fan-out should stay well under 50ms p99 on CI runners.
	if rep.P99Ns > 50_000_000 {
		t.Fatalf("p99 too high for in-process ring: %d ns", rep.P99Ns)
	}
	t.Logf("ring latency p50=%dns p99=%dns p999=%dns n=%d", rep.P50Ns, rep.P99Ns, rep.P999Ns, rep.Count)
}

func TestProof_HTTPFeedAPI(t *testing.T) {
	EnsureInitialized()
	req := httptest.NewRequest(http.MethodGet, "/api/feed", nil)
	rec := httptest.NewRecorder()
	HandleFeedAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["liveEnabled"] != false {
		t.Fatalf("expected liveEnabled=false by default, got %v", payload["liveEnabled"])
	}
	if payload["productId"] != "BTC-USD" {
		t.Fatalf("productId=%v", payload["productId"])
	}
}

func TestProof_ExperimentIncludesPercentiles(t *testing.T) {
	trades := GenerateMockTrades(256)
	res := runSingleBenchmark("RingBufferV6", trades, 4)
	if res.Samples == 0 {
		t.Fatal("expected latency samples on RingBufferV6")
	}
	if res.P50Ns <= 0 || res.P99Ns < res.P50Ns {
		t.Fatalf("bad percentiles: %+v", res)
	}
	t.Logf("RingBufferV6 wall=%dms p50=%dns p99=%dns allocs=%d",
		res.TimeNs/1e6, res.P50Ns, res.P99Ns, res.Allocs)
}

func TestProof_ConcurrentPublishRejected(t *testing.T) {
	TestRingBuffer_SingleWriterEnforced(t)
}

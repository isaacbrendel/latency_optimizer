package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestCompactTradeSize(t *testing.T) {
	if unsafe.Sizeof(CompactTrade{}) != 40 {
		t.Fatalf("CompactTrade must be exactly 40 bytes, got %d", unsafe.Sizeof(CompactTrade{}))
	}
}

func TestOrderBook_L2Calculations(t *testing.T) {
	ob := NewOrderBook()

	ob.Update(ToUSD(65000.0), ToBTC(1.5), 0)
	ob.Update(ToUSD(64990.0), ToBTC(2.0), 0)
	ob.Update(ToUSD(64980.0), ToBTC(3.0), 0)
	ob.Update(ToUSD(65010.0), ToBTC(1.0), 1)
	ob.Update(ToUSD(65020.0), ToBTC(2.5), 1)

	ob.mu.RLock()
	if len(ob.TopBids) != 3 {
		t.Fatalf("expected 3 top bids, got %d", len(ob.TopBids))
	}
	if ob.TopBids[0].Price != ToUSD(65000.0) || ob.TopBids[1].Price != ToUSD(64990.0) {
		t.Errorf("Top bids incorrectly sorted: %v", ob.TopBids)
	}
	if len(ob.TopAsks) != 2 {
		t.Fatalf("expected 2 top asks, got %d", len(ob.TopAsks))
	}
	if ob.TopAsks[0].Price != ToUSD(65010.0) {
		t.Errorf("Top asks incorrectly sorted: %v", ob.TopAsks)
	}
	if ob.Spread != ToUSD(10.0) {
		t.Errorf("expected spread $10, got %v", ob.Spread)
	}
	expectedOBI := 0.30
	if ob.OBI < expectedOBI-0.001 || ob.OBI > expectedOBI+0.001 {
		t.Errorf("expected OBI ~%f, got %f", expectedOBI, ob.OBI)
	}
	ob.mu.RUnlock()

	ob.Update(ToUSD(65000.0), 0, 0)
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if len(ob.TopBids) != 2 {
		t.Fatalf("expected 2 top bids after deletion, got %d", len(ob.TopBids))
	}
	if ob.TopBids[0].Price != ToUSD(64990.0) {
		t.Errorf("expected best bid 64990 after deleting 65000, got %v", ob.TopBids[0].Price)
	}
}

func TestOrderBook_CrossingBookResolution(t *testing.T) {
	ob := NewOrderBook()
	ob.Update(ToUSD(65000.0), ToBTC(1.0), 0)
	ob.Update(ToUSD(65010.0), ToBTC(1.0), 1)
	ob.Update(ToUSD(65020.0), ToBTC(1.0), 0) // crosses

	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if len(ob.TopBids) > 0 && len(ob.TopAsks) > 0 {
		if ob.TopBids[0].Price >= ob.TopAsks[0].Price {
			t.Errorf("crossed book was not resolved: Best Bid %v >= Best Ask %v",
				ob.TopBids[0].Price, ob.TopAsks[0].Price)
		}
	}
}

func TestOrderBook_LogNUpdatesUnderLoad(t *testing.T) {
	ob := NewOrderBook()
	for i := 0; i < 500; i++ {
		ob.Update(ToUSD(60000+float64(i)), ToBTC(0.1), 0)
		ob.Update(ToUSD(70000+float64(i)), ToBTC(0.1), 1)
	}
	bids, asks := ob.LevelCount()
	if bids != 500 || asks != 500 {
		t.Fatalf("expected 500/500 levels, got %d/%d", bids, asks)
	}
	// Delete half
	for i := 0; i < 250; i++ {
		ob.Update(ToUSD(60000+float64(i)), 0, 0)
	}
	bids, _ = ob.LevelCount()
	if bids != 250 {
		t.Fatalf("expected 250 bids after deletes, got %d", bids)
	}
}

func TestMicrostructure_VWAPAndOFI(t *testing.T) {
	m := NewMicrostructureState()
	ob := NewOrderBook()
	ob.Update(ToUSD(100.0), ToBTC(1.0), 0)
	ob.Update(ToUSD(101.0), ToBTC(1.0), 1)
	m.OnBookUpdate(ob)

	// Move bid up → positive OFI contribution
	ob.Update(ToUSD(100.5), ToBTC(2.0), 0)
	m.OnBookUpdate(ob)
	snap := m.Snapshot()
	if snap["ofi"].(float64) == 0 {
		t.Fatalf("expected non-zero OFI after BBO change, got %v", snap["ofi"])
	}

	m.OnTrade(ToUSD(100.0), ToBTC(1.0))
	m.OnTrade(ToUSD(200.0), ToBTC(1.0))
	snap = m.Snapshot()
	vwap := snap["vwap"].(USD)
	if vwap != ToUSD(150.0) {
		t.Fatalf("expected VWAP 150, got %v (%f)", vwap, vwap.Float64())
	}
}

func TestMicrostructure_RSINotDisguisedOBI(t *testing.T) {
	m := NewMicrostructureState()
	ob := NewOrderBook()
	// Seed RSI with rising mids
	price := 100.0
	for i := 0; i < 20; i++ {
		price += 1.0
		ob.Update(ToUSD(price), ToBTC(1), 0)
		ob.Update(ToUSD(price+1), ToBTC(1), 1)
		m.OnBookUpdate(ob)
	}
	rsi := m.Snapshot()["rsi"].(float64)
	obi := m.Snapshot()["obi"].(float64)
	// Rising market → RSI should be high; must not equal (obi+1)*50
	fake := (obi + 1.0) * 50.0
	if abs(rsi-fake) < 1e-9 && abs(obi) > 0.01 {
		t.Fatalf("RSI looks like disguised OBI: rsi=%f fake=%f obi=%f", rsi, fake, obi)
	}
	if rsi < 50 {
		t.Fatalf("expected RSI > 50 on rising mids, got %f", rsi)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestEventTrace_CircularCap(t *testing.T) {
	for i := 1; i <= 30; i++ {
		AddTrace("W", "WRITE", int64(i%32), int64(i), "Test Event")
	}
	recent := GetTraces()
	if len(recent) > 20 {
		t.Fatalf("expected at most 20 traces, got %d", len(recent))
	}
	if recent[len(recent)-1].TradeID != 30 {
		t.Errorf("expected last trace trade ID 30, got %d", recent[len(recent)-1].TradeID)
	}
}

func TestHTTP_OrderBookAPI(t *testing.T) {
	EnsureInitialized()
	req := httptest.NewRequest(http.MethodGet, "/api/orderbook", nil)
	rec := httptest.NewRecorder()
	HandleOrderBookAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}
	var payload struct {
		MidPrice USD      `json:"midPrice"`
		Bot      BotState `json:"bot"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode /api/orderbook JSON: %v", err)
	}
	if payload.Bot.Cash == 0 && payload.Bot.NAV == 0 {
		t.Errorf("bot state in response is empty: %+v", payload.Bot)
	}
}

func TestHTTP_RingBufferAPI(t *testing.T) {
	EnsureInitialized()
	req := httptest.NewRequest(http.MethodGet, "/api/ring-buffer", nil)
	rec := httptest.NewRecorder()
	HandleRingBufferAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}
	var payload struct {
		WriteSeq int64 `json:"writeSeq"`
		UISeq    int64 `json:"uiSeq"`
		Slots    []struct {
			Index int64 `json:"index"`
		} `json:"slots"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(payload.Slots) != 32 {
		t.Errorf("expected 32 slots, got %d", len(payload.Slots))
	}
}

func TestHTTP_SentimentAPI(t *testing.T) {
	EnsureInitialized()
	req := httptest.NewRequest(http.MethodGet, "/api/sentiment", nil)
	rec := httptest.NewRecorder()
	HandleSentimentAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}
	var sentiment map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &sentiment); err != nil {
		t.Fatalf("failed to decode sentiment: %v", err)
	}
	for _, key := range []string{"vwap", "rsi", "obi", "ofi"} {
		if _, ok := sentiment[key]; !ok {
			t.Errorf("missing key %s in sentiment payload", key)
		}
	}
}

func TestHTTP_RunExperimentAPI(t *testing.T) {
	EnsureInitialized()
	req := httptest.NewRequest(http.MethodGet, "/api/run-experiment?trades=100&subscribers=10", nil)
	rec := httptest.NewRecorder()
	HandleRunExperimentAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Errorf("expected text/event-stream")
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Errorf("expected [DONE] in SSE body")
	}
}

func TestRingBuffer_EngineWaitStrategies(t *testing.T) {
	rb := NewRingBufferV6(64, 1)
	rb.SetWaitStrategy("Adaptive")
	rb.PublishBatch([]CompactTrade{{ID: 1, Price: ToUSD(100)}, {ID: 2, Price: ToUSD(200)}})
	var readCount int64
	rb.Read(rb.Readers[0], 2, nil, func(ct CompactTrade) { atomic.AddInt64(&readCount, 1) })
	if atomic.LoadInt64(&readCount) != 2 {
		t.Errorf("expected 2 trades, got %d", readCount)
	}
}

func TestRingBuffer_SingleWriterEnforced(t *testing.T) {
	rb := NewRingBufferV6(64, 1)
	started := make(chan struct{})
	block := make(chan struct{})
	go func() {
		rb.beginPublish()
		close(started)
		<-block
		rb.endPublish()
	}()
	<-started
	defer close(block)

	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		rb.PublishBatch([]CompactTrade{{ID: 1}})
	}()
	if !panicked {
		t.Fatal("expected panic on concurrent publish")
	}
}

func TestRingBuffer_RaceFreeEviction(t *testing.T) {
	rb := NewRingBufferV6(8, 2)
	rb.Readers[0].Blocking = false
	rb.Readers[1].Blocking = false

	const target int64 = 1 << 20
	var slowN, fastN int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rb.Read(rb.Readers[0], target, nil, func(ct CompactTrade) {
			atomic.AddInt64(&slowN, 1)
			time.Sleep(3 * time.Millisecond)
		})
	}()
	go func() {
		defer wg.Done()
		rb.Read(rb.Readers[1], target, nil, func(ct CompactTrade) {
			atomic.AddInt64(&fastN, 1)
		})
	}()

	time.Sleep(20 * time.Millisecond)

	trades := make([]CompactTrade, 64)
	for i := range trades {
		trades[i] = CompactTrade{ID: int64(i + 1), Price: ToUSD(float64(i))}
	}
	for i := 0; i < len(trades); i += 8 {
		rb.PublishBatchEvicting(trades[i : i+8])
	}

	deadline := time.After(3 * time.Second)
	for atomic.LoadInt64(&rb.Readers[0].EvictedCount) == 0 || atomic.LoadInt64(&fastN) < 8 {
		select {
		case <-deadline:
			t.Fatalf("evicted=%d slowN=%d fastN=%d",
				atomic.LoadInt64(&rb.Readers[0].EvictedCount),
				atomic.LoadInt64(&slowN), atomic.LoadInt64(&fastN))
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Unblock readers
	atomic.StoreInt64(&rb.Readers[0].ReadSeq, target)
	atomic.StoreInt64(&rb.Readers[1].ReadSeq, target)
	rb.WaitStrategy.Signal()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	if atomic.LoadInt64(&fastN) == 0 {
		t.Fatal("fast reader should have received events")
	}
	if atomic.LoadInt64(&rb.Readers[0].EvictedCount) == 0 {
		t.Fatal("slow reader should have been evicted")
	}
}

func TestRingBuffer_NoLossBlockingConsumers(t *testing.T) {
	const n = 5000
	const consumers = 8
	rb := NewRingBufferV6(256, consumers)
	trades := make([]CompactTrade, n)
	for i := range trades {
		trades[i] = CompactTrade{ID: int64(i + 1), Price: USD(i), Quantity: BTC(i)}
	}
	var wg sync.WaitGroup
	wg.Add(consumers)
	received := make([][]int64, consumers)
	for i := 0; i < consumers; i++ {
		received[i] = make([]int64, 0, n)
		go func(idx int, r *RingBufferReader) {
			defer wg.Done()
			rb.Read(r, int64(n), nil, func(ct CompactTrade) {
				received[idx] = append(received[idx], ct.ID)
			})
		}(i, rb.Readers[i])
	}
	for i := 0; i < n; i += 64 {
		end := i + 64
		if end > n {
			end = n
		}
		rb.PublishBatch(trades[i:end])
	}
	wg.Wait()
	for c := 0; c < consumers; c++ {
		if len(received[c]) != n {
			t.Fatalf("consumer %d: got %d want %d", c, len(received[c]), n)
		}
		for i, id := range received[c] {
			if id != int64(i+1) {
				t.Fatalf("consumer %d pos %d: id %d", c, i, id)
			}
		}
	}
}

func TestBinaryWire_RoundTrip(t *testing.T) {
	ct := CompactTrade{
		ID: 42, Price: ToUSD(12345.67), Quantity: ToBTC(0.5),
		Timestamp: 99, Sequence: 7, SymbolID: 1, Side: 1, Flags: 2, VenueID: 2,
	}
	buf := EncodeFlatTrade(nil, ct)
	out := BinaryFlatTrade(buf).DecodeToCompactTrade()
	if out.ID != ct.ID || out.Price != ct.Price || out.Quantity != ct.Quantity || out.Sequence != ct.Sequence {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, ct)
	}
}

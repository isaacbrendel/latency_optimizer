package engine

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestL2Feed_SnapshotAndUpdate(t *testing.T) {
	book := NewOrderBook()
	rb := NewRingBufferV6(64, 1)
	micro := NewMicrostructureState()
	feed := NewL2Feed("BTC-USD", book, rb, micro)

	snap, _ := json.Marshal(map[string]interface{}{
		"type":       "snapshot",
		"product_id": "BTC-USD",
		"sequence":   100,
		"bids":       [][]string{{"65000.00", "1.5"}, {"64990.00", "2.0"}},
		"asks":       [][]string{{"65010.00", "1.0"}, {"65020.00", "2.5"}},
	})
	resync, err := feed.HandleMessage(snap)
	if err != nil || resync {
		t.Fatalf("snapshot failed: resync=%v err=%v", resync, err)
	}
	bids, asks := book.LevelCount()
	if bids != 2 || asks != 2 {
		t.Fatalf("expected 2/2 levels, got %d/%d", bids, asks)
	}
	if feed.Stats().Snapshots != 1 {
		t.Fatalf("expected 1 snapshot")
	}

	upd, _ := json.Marshal(map[string]interface{}{
		"type":       "l2update",
		"product_id": "BTC-USD",
		"sequence":   101,
		"changes":    [][]string{{"buy", "65000.00", "0"}, {"sell", "65015.00", "0.5"}},
	})
	resync, err = feed.HandleMessage(upd)
	if err != nil || resync {
		t.Fatalf("update failed: resync=%v err=%v", resync, err)
	}
	bids, asks = book.LevelCount()
	if bids != 1 {
		t.Fatalf("expected bid deleted, bids=%d", bids)
	}
	if asks < 2 {
		t.Fatalf("expected new ask level, asks=%d", asks)
	}
	if feed.Stats().LastSequence != 101 {
		t.Fatalf("expected last seq 101, got %d", feed.Stats().LastSequence)
	}
}

func TestL2Feed_SequenceGapTriggersResync(t *testing.T) {
	book := NewOrderBook()
	feed := NewL2Feed("BTC-USD", book, nil, nil)

	snap, _ := json.Marshal(map[string]interface{}{
		"type": "snapshot", "product_id": "BTC-USD", "sequence": 10,
		"bids": [][]string{{"100.00", "1"}}, "asks": [][]string{{"101.00", "1"}},
	})
	_, _ = feed.HandleMessage(snap)

	gap, _ := json.Marshal(map[string]interface{}{
		"type": "l2update", "product_id": "BTC-USD", "sequence": 15, // gap: expected 11
		"changes": [][]string{{"buy", "100.00", "2"}},
	})
	resync, err := feed.HandleMessage(gap)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !resync {
		t.Fatal("expected resync=true on sequence gap")
	}
	if feed.Stats().Gaps != 1 {
		t.Fatalf("expected gaps=1, got %d", feed.Stats().Gaps)
	}

	// Deltas while needSnap should be ignored
	upd, _ := json.Marshal(map[string]interface{}{
		"type": "l2update", "product_id": "BTC-USD", "sequence": 16,
		"changes": [][]string{{"buy", "99.00", "1"}},
	})
	resync, _ = feed.HandleMessage(upd)
	if resync {
		t.Fatal("should not resync again while waiting for snapshot")
	}
	bids, _ := book.LevelCount()
	if bids != 1 {
		t.Fatalf("book should be unchanged while awaiting snapshot, bids=%d", bids)
	}
}

func TestL2Feed_DuplicateSequenceIgnored(t *testing.T) {
	book := NewOrderBook()
	feed := NewL2Feed("BTC-USD", book, nil, nil)
	snap, _ := json.Marshal(map[string]interface{}{
		"type": "snapshot", "sequence": 5,
		"bids": [][]string{{"100.00", "1"}}, "asks": [][]string{{"101.00", "1"}},
	})
	_, _ = feed.HandleMessage(snap)
	dup, _ := json.Marshal(map[string]interface{}{
		"type": "l2update", "sequence": 5,
		"changes": [][]string{{"buy", "100.00", "9"}},
	})
	_, _ = feed.HandleMessage(dup)
	book.mu.RLock()
	sz := book.Bids[0].Size
	book.mu.RUnlock()
	if sz != ToBTC(1) {
		t.Fatalf("duplicate should not apply, size=%v", sz)
	}
}

func TestSubscribeMessage(t *testing.T) {
	raw := SubscribeMessage("BTC-USD", "ETH-USD")
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["type"] != "subscribe" {
		t.Fatalf("type=%v", m["type"])
	}
}

func TestLatencyHistogram_Percentiles(t *testing.T) {
	h := NewLatencyHistogram(1000)
	for i := int64(1); i <= 100; i++ {
		h.Record(i * 1000) // 1µs .. 100µs
	}
	p50, p90, p99, p999, n := h.Percentiles()
	if n != 100 {
		t.Fatalf("n=%d", n)
	}
	if p50 < 40_000 || p50 > 60_000 {
		t.Fatalf("p50=%d", p50)
	}
	if p90 < 80_000 || p90 > 100_000 {
		t.Fatalf("p90=%d", p90)
	}
	if p99 < 90_000 {
		t.Fatalf("p99=%d", p99)
	}
	if p999 < p99 {
		t.Fatalf("p999=%d < p99=%d", p999, p99)
	}
	rep := h.Report()
	if rep.Count != 100 || rep.P50Ns != p50 {
		t.Fatalf("report mismatch: %+v", rep)
	}
}

func TestRingBuffer_ChaosSlowStart(t *testing.T) {
	// Publisher starts before readers attach readiness — still zero loss for blocking.
	const n = 2000
	rb := NewRingBufferV6(128, 4)
	trades := make([]CompactTrade, n)
	for i := range trades {
		trades[i] = CompactTrade{ID: int64(i + 1), Price: USD(i)}
	}

	donePub := make(chan struct{})
	go func() {
		for i := 0; i < n; i += 32 {
			end := i + 32
			if end > n {
				end = n
			}
			rb.PublishBatch(trades[i:end])
		}
		close(donePub)
	}()

	got := make([]int64, 4)
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(idx int) {
			defer wg.Done()
			var c int64
			rb.Read(rb.Readers[idx], int64(n), nil, func(ct CompactTrade) { c++ })
			got[idx] = c
		}(i)
	}
	<-donePub
	wg.Wait()
	for i, c := range got {
		if c != n {
			t.Fatalf("reader %d got %d want %d", i, c, n)
		}
	}
}

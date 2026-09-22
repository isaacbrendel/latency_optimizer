package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestApplyRESTBook_TruncatesAndApplies(t *testing.T) {
	book := NewOrderBook()
	ring := NewRingBufferV6(256, 1)
	micro := NewMicrostructureState()
	feed := NewL2Feed("BTC-USD", book, ring, micro)

	// Match live Exchange JSON: price/size strings, num_orders as number.
	bids := make([][]interface{}, 80)
	asks := make([][]interface{}, 80)
	for i := 0; i < 80; i++ {
		bids[i] = []interface{}{formatPrice(100000 - float64(i)), "0.1", 1}
		asks[i] = []interface{}{formatPrice(100001 + float64(i)), "0.2", 1}
	}
	raw, _ := json.Marshal(map[string]interface{}{
		"sequence": 42,
		"bids":     bids,
		"asks":     asks,
	})
	if err := ApplyRESTBook(feed, raw); err != nil {
		t.Fatal(err)
	}
	stats := feed.Stats()
	if stats.Snapshots != 1 {
		t.Fatalf("snapshots=%d", stats.Snapshots)
	}
	if stats.LastSequence != 42 {
		t.Fatalf("seq=%d", stats.LastSequence)
	}
	topBids, topAsks, _, mid, _ := book.Snapshot()
	bidN, askN := book.LevelCount()
	if bidN != restBookDepth || askN != restBookDepth {
		t.Fatalf("depth bids=%d asks=%d want %d", bidN, askN, restBookDepth)
	}
	if len(topBids) == 0 || len(topAsks) == 0 {
		t.Fatal("expected top-of-book levels")
	}
	if mid.Float64() < 100000 || mid.Float64() > 100002 {
		t.Fatalf("unexpected mid %v", mid)
	}
	microSnap := micro.Snapshot()
	if microSnap["obi"] == nil {
		t.Fatal("expected microstructure update from REST book")
	}
}

func formatPrice(p float64) string {
	b, _ := json.Marshal(p)
	return string(b)
}

func TestRefreshFromREST_HTTPServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/products/BTC-USD/book" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"sequence": 99,
			"bids":     [][]string{{"85000.5", "1.5", "2"}},
			"asks":     [][]string{{"85001.0", "0.8", "1"}},
		})
	}))
	defer srv.Close()

	t.Setenv("EXCHANGE_REST_URL", srv.URL)
	t.Setenv("DISABLE_LIVE_FEED", "")

	book := NewOrderBook()
	feed := NewL2Feed("BTC-USD", book, NewRingBufferV6(64, 1), NewMicrostructureState())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RefreshFromREST(ctx, feed); err != nil {
		t.Fatal(err)
	}
	if FeedStatus(feed.Stats().Status) != FeedStatusLiveREST {
		t.Fatalf("status=%s", FeedStatus(feed.Stats().Status))
	}
	if atomic.LoadInt64(&restLastNS) == 0 {
		t.Fatal("expected restLastNS set")
	}
	bids, asks, _, mid, _ := book.Snapshot()
	if len(bids) != 1 || bids[0].Price.Float64() != 85000.5 {
		t.Fatalf("bids=%+v", bids)
	}
	if len(asks) != 1 || mid.Float64() < 85000 {
		t.Fatalf("asks=%+v mid=%v", asks, mid)
	}
}

func TestRestFeedEnabled(t *testing.T) {
	t.Setenv("DISABLE_LIVE_FEED", "")
	if !RestFeedEnabled() {
		t.Fatal("expected REST on by default")
	}
	t.Setenv("DISABLE_LIVE_FEED", "1")
	if RestFeedEnabled() {
		t.Fatal("DISABLE_LIVE_FEED should disable REST")
	}
}

func TestRefreshFromREST_LiveExchange(t *testing.T) {
	if testing.Short() {
		t.Skip("skip live exchange call in -short")
	}
	t.Setenv("EXCHANGE_REST_URL", defaultExchangeREST)
	t.Setenv("DISABLE_LIVE_FEED", "")

	book := NewOrderBook()
	feed := NewL2Feed("BTC-USD", book, NewRingBufferV6(256, 1), NewMicrostructureState())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := RefreshFromREST(ctx, feed); err != nil {
		t.Fatalf("live REST failed: %v", err)
	}
	bids, asks, _, mid, _ := book.Snapshot()
	if len(bids) == 0 || len(asks) == 0 {
		t.Fatal("expected live book levels")
	}
	m := mid.Float64()
	if m < 1000 || m > 5_000_000 {
		t.Fatalf("implausible BTC mid %f", m)
	}
	t.Logf("live BTC-USD mid≈%.2f seq=%d", m, feed.Stats().LastSequence)
}

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultExchangeREST = "https://api.exchange.coinbase.com"
	restBookDepth         = 50
	restRefreshTTL        = 1500 * time.Millisecond
)

// RestFeedEnabled is true unless DISABLE_LIVE_FEED=1.
// Used on serverless (Vercel) where persistent WebSockets cannot stay open.
func RestFeedEnabled() bool {
	return os.Getenv("DISABLE_LIVE_FEED") != "1"
}

// LiveRESTURL returns the Exchange REST base (overridable via EXCHANGE_REST_URL).
func LiveRESTURL() string {
	if u := os.Getenv("EXCHANGE_REST_URL"); u != "" {
		return u
	}
	return defaultExchangeREST
}

var (
	restMu       sync.Mutex
	restLastNS   int64
	restHTTPClient = &http.Client{Timeout: 8 * time.Second}
)

// EnsureFreshMarketData refreshes the L2 book from Coinbase REST when the
// WebSocket is not connected and the last REST snapshot is older than TTL.
// Safe to call on every API request (serverless-friendly).
func EnsureFreshMarketData() {
	if !RestFeedEnabled() {
		return
	}
	if atomic.LoadInt32(&wsConnected) == 1 {
		return
	}
	last := atomic.LoadInt64(&restLastNS)
	if last > 0 && time.Since(time.Unix(0, last)) < restRefreshTTL {
		return
	}
	if liveFeed == nil {
		return
	}
	restMu.Lock()
	defer restMu.Unlock()
	// Double-check after lock.
	last = atomic.LoadInt64(&restLastNS)
	if last > 0 && time.Since(time.Unix(0, last)) < restRefreshTTL {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := RefreshFromREST(ctx, liveFeed); err != nil {
		AddTrace("W", "WRITE", 0, 0, "REST L2 refresh failed: "+err.Error())
		return
	}
}

// RefreshFromREST fetches a public L2 book snapshot and applies it to the feed.
func RefreshFromREST(ctx context.Context, feed *L2Feed) error {
	if feed == nil {
		return fmt.Errorf("nil feed")
	}
	product := feed.ProductID
	if product == "" {
		product = "BTC-USD"
	}
	url := fmt.Sprintf("%s/products/%s/book?level=2", LiveRESTURL(), product)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "latency-optimizer/1.0")

	resp, err := restHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REST book HTTP %d: %s", resp.StatusCode, truncate(string(body), 120))
	}
	if err := ApplyRESTBook(feed, body); err != nil {
		return err
	}
	atomic.StoreInt64(&restLastNS, time.Now().UnixNano())
	feed.SetStatus(FeedStatusLiveREST)
	return nil
}

// ApplyRESTBook converts a Coinbase Exchange REST /products/{id}/book payload
// into an L2 snapshot and applies it (top restBookDepth levels per side).
// REST levels are [price, size, num_orders] where price/size are strings and
// num_orders is a number — decode via []interface{} then normalize.
func ApplyRESTBook(feed *L2Feed, raw []byte) error {
	var book struct {
		Sequence int64           `json:"sequence"`
		Bids     [][]interface{} `json:"bids"`
		Asks     [][]interface{} `json:"asks"`
	}
	if err := json.Unmarshal(raw, &book); err != nil {
		atomic.AddInt64(&feed.stats.ParseErrors, 1)
		return err
	}
	if len(book.Bids) == 0 && len(book.Asks) == 0 {
		return fmt.Errorf("empty REST book")
	}
	if len(book.Bids) > restBookDepth {
		book.Bids = book.Bids[:restBookDepth]
	}
	if len(book.Asks) > restBookDepth {
		book.Asks = book.Asks[:restBookDepth]
	}

	// Reuse WS snapshot applicator (same [[price,size], ...] shape).
	snap := map[string]interface{}{
		"type":       "snapshot",
		"product_id": feed.ProductID,
		"bids":       normalizeRESTLevels(book.Bids),
		"asks":       normalizeRESTLevels(book.Asks),
		"sequence":   book.Sequence,
	}
	encoded, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	_, err = feed.HandleMessage(encoded)
	return err
}

func normalizeRESTLevels(levels [][]interface{}) [][]string {
	out := make([][]string, 0, len(levels))
	for _, lvl := range levels {
		if len(lvl) < 2 {
			continue
		}
		px, ok1 := jsonNumberString(lvl[0])
		sz, ok2 := jsonNumberString(lvl[1])
		if !ok1 || !ok2 {
			continue
		}
		out = append(out, []string{px, sz})
	}
	return out
}

func jsonNumberString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		if t == "" {
			return "", false
		}
		return t, true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case json.Number:
		return t.String(), true
	default:
		return "", false
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// runRESTSnapshotPoller keeps the book fresh on long-running hosts when the
// WebSocket is down. On serverless, EnsureFreshMarketData covers cold starts.
func runRESTSnapshotPoller(ctx context.Context, feed *L2Feed) {
	if !RestFeedEnabled() {
		return
	}
	ticker := time.NewTicker(restRefreshTTL)
	defer ticker.Stop()

	// Immediate attempt so first paint is real even before the first tick.
	if atomic.LoadInt32(&wsConnected) == 0 {
		rctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		_ = RefreshFromREST(rctx, feed)
		cancel()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if atomic.LoadInt32(&wsConnected) == 1 {
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			_ = RefreshFromREST(rctx, feed)
			cancel()
		}
	}
}

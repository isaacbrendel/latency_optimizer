package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OrderBookState is the live L2 book (sorted-ladder, O(log n) updates).
var OrderBookState = NewOrderBook()

// IndicatorStateVal holds honest VWAP / RSI / OBI / OFI microstructure metrics.
var IndicatorStateVal = NewMicrostructureState()

type EventTrace struct {
	Timestamp string `json:"timestamp"`
	Actor     string `json:"actor"`  // W, UI, IND, BOT
	Action    string `json:"action"` // WRITE, READ
	Slot      int64  `json:"slot"`
	TradeID   int64  `json:"tradeId"`
	Details   string `json:"details"`
}

var (
	traceMu      sync.Mutex
	traces       = make([]EventTrace, 0)
	wsConnected  int32
	historyMu    sync.Mutex
	tradeHistory = make([]CompactTrade, 0)
)

func AddTrace(actor, action string, slot, tradeID int64, details string) {
	traceMu.Lock()
	defer traceMu.Unlock()
	t := EventTrace{
		Timestamp: time.Now().Format("15:04:05.000"),
		Actor:     actor,
		Action:    action,
		Slot:      slot,
		TradeID:   tradeID,
		Details:   details,
	}
	traces = append(traces, t)
	if len(traces) > 20 {
		traces = traces[len(traces)-20:]
	}
}

func GetTraces() []EventTrace {
	traceMu.Lock()
	defer traceMu.Unlock()
	copied := make([]EventTrace, len(traces))
	copy(copied, traces)
	return copied
}

type LiveState struct {
	mu                sync.RWMutex
	rb                *RingBufferV6
	dashboardReader   *RingBufferReader
	quantReader       *RingBufferReader
	botReader         *RingBufferReader
	auditReader       *RingBufferReader
	lastTradeID       int64
	recentTradesFloat []*Trade
	waitStrategy      string
}

var EngineState LiveState

type BotOrder struct {
	ID        int64  `json:"id"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"` // BUY, SELL, STOP_LOSS, TAKE_PROFIT
	Price     USD    `json:"price"`
	Quantity  BTC    `json:"quantity"`
	Value     USD    `json:"value"`
}

type BotState struct {
	mu            sync.Mutex
	Cash          USD        `json:"cash"`
	Position      BTC        `json:"position"`
	NAV           USD        `json:"nav"`
	BuyAndHoldNAV USD        `json:"buyAndHoldNav"`
	InitialPrice  USD        `json:"initialPrice"`
	FastEMA       USD        `json:"fastEma"`
	SlowEMA       USD        `json:"slowEma"`
	Signal        string     `json:"signal"`
	Orders        []BotOrder `json:"orders"`
	OrderCounter  int64      `json:"-"`
	StopLossPct   float64    `json:"stopLossPct"`
	TakeProfitPct float64    `json:"takeProfitPct"`
	TakerFeePct   float64    `json:"takerFeePct"`
	SlippagePct   float64    `json:"slippagePct"`
	EntryPrice    USD        `json:"entryPrice"`
	EntryTime     int64      `json:"-"`
	Strategy      string     `json:"strategy"`
	Commentary    string     `json:"commentary"`
}

var BotStateVal BotState

var (
	initOnce   sync.Once
	liveFeed   *L2Feed
	feedCancel context.CancelFunc
)

func EnsureInitialized() {
	initOnce.Do(func() {
		initEngineState()
		ctx, cancel := context.WithCancel(context.Background())
		feedCancel = cancel
		liveFeed = NewL2Feed("BTC-USD", OrderBookState, EngineState.rb, IndicatorStateVal)
		go RunLiveL2Feed(ctx, liveFeed)
		go runMockL2Producer()
		go runDashboardConsumer()
		go runQuantConsumer()
		go runAuditConsumer()
		go runBotConsumer()
	})
}

func seedInitialMarketData() {
	OrderBookState.mu.RLock()
	hasData := len(OrderBookState.TopBids) > 0
	OrderBookState.mu.RUnlock()
	if hasData {
		return
	}
	r := rand.New(rand.NewSource(42))
	lastPrice := 65000.0

	var snapshotBatch []CompactTrade
	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		bidPrice := lastPrice - float64(i)*2.0 - 0.5 - r.Float64()
		bidSize := 0.5 + r.Float64()*3.0
		snapshotBatch = append(snapshotBatch, CompactTrade{
			ID:        int64(i + 1),
			Price:     ToUSD(bidPrice),
			Quantity:  ToBTC(bidSize),
			Timestamp: now,
			SymbolID:  0,
			Side:      0,
			VenueID:   uint8(i % 3),
		})

		askPrice := lastPrice + float64(i)*2.0 + 0.5 + r.Float64()
		askSize := 0.5 + r.Float64()*3.0
		snapshotBatch = append(snapshotBatch, CompactTrade{
			ID:        int64(i + 21),
			Price:     ToUSD(askPrice),
			Quantity:  ToBTC(askSize),
			Timestamp: now,
			SymbolID:  0,
			Side:      1,
			VenueID:   uint8(i % 3),
		})
	}
	EngineState.rb.PublishBatch(snapshotBatch)
	for _, t := range snapshotBatch {
		OrderBookState.Update(t.Price, t.Quantity, t.Side)
	}
	IndicatorStateVal.OnBookUpdate(OrderBookState)
	AddTrace("W", "WRITE", 0, 0, "[Multi-Venue Snapshot] Bids: 20, Asks: 20")
}

func initEngineState() {
	EngineState.rb = NewRingBufferV6(2048, 4)
	EngineState.dashboardReader = EngineState.rb.Readers[0]
	EngineState.dashboardReader.Blocking = false
	EngineState.quantReader = EngineState.rb.Readers[1]
	EngineState.botReader = EngineState.rb.Readers[2]
	EngineState.auditReader = EngineState.rb.Readers[3]
	EngineState.recentTradesFloat = make([]*Trade, 0)
	EngineState.waitStrategy = "Blocking"

	BotStateVal.mu.Lock()
	BotStateVal.Cash = ToUSD(100000.0)
	BotStateVal.Position = 0
	BotStateVal.NAV = ToUSD(100000.0)
	BotStateVal.BuyAndHoldNAV = ToUSD(100000.0)
	BotStateVal.InitialPrice = 0
	BotStateVal.Orders = make([]BotOrder, 0)
	BotStateVal.Signal = "HOLD"
	BotStateVal.StopLossPct = 0.005
	BotStateVal.TakeProfitPct = 0.012
	BotStateVal.TakerFeePct = 0.0005
	BotStateVal.SlippagePct = 0.0001
	BotStateVal.Strategy = "OBI"
	BotStateVal.Commentary = "Waiting for next HFT signal cycle..."
	BotStateVal.mu.Unlock()

	seedInitialMarketData()
}

func runMockL2Producer() {
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	lastPrice := 65000.0

	var snapshotBatch []CompactTrade
	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		bidPrice := lastPrice - float64(i)*2.0 - r.Float64()
		bidSize := 0.1 + r.Float64()*4.0
		snapshotBatch = append(snapshotBatch, CompactTrade{
			ID:        int64(i + 1),
			Price:     ToUSD(bidPrice),
			Quantity:  ToBTC(bidSize),
			Timestamp: now,
			SymbolID:  0,
			Side:      0,
			VenueID:   uint8(i % 3),
		})

		askPrice := lastPrice + float64(i)*2.0 + r.Float64()
		askSize := 0.1 + r.Float64()*4.0
		snapshotBatch = append(snapshotBatch, CompactTrade{
			ID:        int64(i + 21),
			Price:     ToUSD(askPrice),
			Quantity:  ToBTC(askSize),
			Timestamp: now,
			SymbolID:  0,
			Side:      1,
			VenueID:   uint8(i % 3),
		})
	}
	EngineState.rb.PublishBatch(snapshotBatch)
	AddTrace("W", "WRITE", 0, 0, "[Multi-Venue Snapshot] Bids: 20, Asks: 20")

	for range ticker.C {
		if atomic.LoadInt32(&wsConnected) == 1 {
			return
		}

		lastPrice += (r.Float64() - 0.5) * 5.0

		var updates []CompactTrade
		nowUpdate := time.Now().UnixNano()

		numUpdates := 1 + r.Intn(4)
		for i := 0; i < numUpdates; i++ {
			side := uint8(0)
			if r.Float64() > 0.5 {
				side = 1
			}

			var price float64
			if side == 0 {
				price = lastPrice - float64(r.Intn(10))*2.0
			} else {
				price = lastPrice + float64(r.Intn(10))*2.0
			}

			var size float64
			if r.Float64() < 0.15 {
				size = 0.0
			} else {
				size = 0.05 + r.Float64()*3.0
			}

			venueID := uint8(r.Intn(3))
			updates = append(updates, CompactTrade{
				ID:        nowUpdate/1e6 + int64(i),
				Price:     ToUSD(price),
				Quantity:  ToBTC(size),
				Timestamp: nowUpdate,
				SymbolID:  0,
				Side:      side,
				VenueID:   venueID,
			})
		}

		EngineState.rb.PublishBatch(updates)
		writeSeq := atomic.LoadInt64(&EngineState.rb.WriteSeq)
		AddTrace("W", "WRITE", (writeSeq-1)%32, 0, fmt.Sprintf("[Fallback L2 Update] changes: %d, mid: $%.2f", len(updates), lastPrice))

		if r.Float64() < 0.3 {
			matchPrice := lastPrice + (r.Float64()-0.5)*2.0
			matchQty := 0.01 + r.Float64()*1.0
			EngineState.mu.Lock()
			tCompat := &Trade{
				ID:        time.Now().UnixNano() / 1e6,
				Price:     matchPrice,
				Quantity:  matchQty,
				Timestamp: nowUpdate,
				Symbol:    "BTC-USD",
				Exchange:  "COINBASE",
			}
			EngineState.recentTradesFloat = append(EngineState.recentTradesFloat, tCompat)
			if len(EngineState.recentTradesFloat) > 50 {
				EngineState.recentTradesFloat = EngineState.recentTradesFloat[len(EngineState.recentTradesFloat)-50:]
			}
			EngineState.mu.Unlock()
			IndicatorStateVal.OnTrade(ToUSD(matchPrice), ToBTC(matchQty))
		}
	}
}

// runDashboardConsumer is the non-blocking UI reader — advances its own cursor
// so eviction metrics and uiSeq reflect a real consumer, not a synthetic writeSeq.
func runDashboardConsumer() {
	var seq int64
	EngineState.rb.Read(EngineState.dashboardReader, 1<<62, nil, func(t CompactTrade) {
		AddTrace("UI", "READ", seq%32, t.ID, fmt.Sprintf("dashboard slot seq=%d", seq))
		seq++
	})
}

func runQuantConsumer() {
	var seq int64 = 0
	EngineState.rb.Read(EngineState.quantReader, 1<<62, nil, func(t CompactTrade) {
		OrderBookState.Update(t.Price, t.Quantity, t.Side)
		IndicatorStateVal.OnBookUpdate(OrderBookState)
		AddTrace("IND", "READ", seq%32, t.ID, fmt.Sprintf("L2 Update: %s $%.2f size %.4f",
			func() string {
				if t.Side == 0 {
					return "BID"
				}
				return "ASK"
			}(),
			t.Price.Float64(), t.Quantity.Float64()))
		seq++
	})
}

func runAuditConsumer() {
	var seq int64 = 0
	EngineState.rb.Read(EngineState.auditReader, 1<<62, nil, func(t CompactTrade) {
		seq++
		_ = t
	})
}

func runBotConsumer() {
	var seq int64 = 0
	barrier := NewSequenceBarrier(func() int64 {
		return atomic.LoadInt64(&EngineState.quantReader.ReadSeq)
	})

	EngineState.rb.Read(EngineState.botReader, 1<<62, barrier, func(t CompactTrade) {
		OrderBookState.mu.RLock()
		obi := OrderBookState.OBI
		var midPrice USD = 0
		if len(OrderBookState.TopBids) > 0 && len(OrderBookState.TopAsks) > 0 {
			midPrice = (OrderBookState.TopBids[0].Price + OrderBookState.TopAsks[0].Price) / 2
		}
		OrderBookState.mu.RUnlock()

		if midPrice == 0 {
			return
		}

		AddTrace("BOT", "READ", seq%32, 0, fmt.Sprintf("OBI HFT check: OBI=%.2f%% at $%.2f", obi*100.0, midPrice.Float64()))
		seq++

		BotStateVal.mu.Lock()
		defer BotStateVal.mu.Unlock()

		if BotStateVal.InitialPrice == 0 {
			BotStateVal.InitialPrice = midPrice
		}

		if BotStateVal.Position > 0 {
			posVal := BotStateVal.Position.Value(midPrice)
			costVal := BotStateVal.Position.Value(BotStateVal.EntryPrice)

			var pnlPct float64 = 0
			if costVal > 0 {
				pnlPct = float64(posVal-costVal) / float64(costVal)
			}

			if pnlPct <= -BotStateVal.StopLossPct {
				slippage := USD(float64(posVal) * BotStateVal.SlippagePct)
				execPrice := midPrice - USD(float64(midPrice)*BotStateVal.SlippagePct)
				fee := USD(float64(posVal) * BotStateVal.TakerFeePct)
				proceeds := BotStateVal.Position.Value(execPrice) - fee - slippage

				BotStateVal.Cash += proceeds
				BotStateVal.OrderCounter++
				order := BotOrder{
					ID:        BotStateVal.OrderCounter,
					Timestamp: time.Now().Format("15:04:05"),
					Type:      "STOP_LOSS",
					Price:     execPrice,
					Quantity:  BotStateVal.Position,
					Value:     proceeds,
				}
				BotStateVal.Orders = append(BotStateVal.Orders, order)
				BotStateVal.Position = 0
				BotStateVal.Signal = "STOP_LOSS"
				BotStateVal.Commentary = fmt.Sprintf("STOP LOSS triggered at $%.2f (PnL: %.2f%%)", execPrice.Float64(), pnlPct*100)
			} else if pnlPct >= BotStateVal.TakeProfitPct {
				slippage := USD(float64(posVal) * BotStateVal.SlippagePct)
				execPrice := midPrice - USD(float64(midPrice)*BotStateVal.SlippagePct)
				fee := USD(float64(posVal) * BotStateVal.TakerFeePct)
				proceeds := BotStateVal.Position.Value(execPrice) - fee - slippage

				BotStateVal.Cash += proceeds
				BotStateVal.OrderCounter++
				order := BotOrder{
					ID:        BotStateVal.OrderCounter,
					Timestamp: time.Now().Format("15:04:05"),
					Type:      "TAKE_PROFIT",
					Price:     execPrice,
					Quantity:  BotStateVal.Position,
					Value:     proceeds,
				}
				BotStateVal.Orders = append(BotStateVal.Orders, order)
				BotStateVal.Position = 0
				BotStateVal.Signal = "TAKE_PROFIT"
				BotStateVal.Commentary = fmt.Sprintf("TAKE PROFIT triggered at $%.2f (PnL: +%.2f%%)", execPrice.Float64(), pnlPct*100)
			}
		}

		if obi > 0.25 && BotStateVal.Position == 0 && BotStateVal.Cash > 0 {
			tradeCash := USD(float64(BotStateVal.Cash) * 0.95)
			execPrice := midPrice + USD(float64(midPrice)*BotStateVal.SlippagePct)
			fee := USD(float64(tradeCash) * BotStateVal.TakerFeePct)
			buyVal := tradeCash - fee

			boughtQty := buyVal.Quant(execPrice)
			if boughtQty > 0 {
				BotStateVal.Cash -= tradeCash
				BotStateVal.Position = boughtQty
				BotStateVal.EntryPrice = execPrice
				BotStateVal.EntryTime = time.Now().Unix()
				BotStateVal.OrderCounter++

				order := BotOrder{
					ID:        BotStateVal.OrderCounter,
					Timestamp: time.Now().Format("15:04:05"),
					Type:      "BUY",
					Price:     execPrice,
					Quantity:  boughtQty,
					Value:     buyVal,
				}
				BotStateVal.Orders = append(BotStateVal.Orders, order)
				BotStateVal.Signal = "BUY"
				BotStateVal.Commentary = fmt.Sprintf("BUY Order Executed at $%.2f (OBI: +%.1f%%)", execPrice.Float64(), obi*100)
			}
		} else if obi < -0.25 && BotStateVal.Position > 0 {
			posVal := BotStateVal.Position.Value(midPrice)
			execPrice := midPrice - USD(float64(midPrice)*BotStateVal.SlippagePct)
			fee := USD(float64(posVal) * BotStateVal.TakerFeePct)
			proceeds := BotStateVal.Position.Value(execPrice) - fee

			BotStateVal.Cash += proceeds
			BotStateVal.OrderCounter++
			order := BotOrder{
				ID:        BotStateVal.OrderCounter,
				Timestamp: time.Now().Format("15:04:05"),
				Type:      "SELL",
				Price:     execPrice,
				Quantity:  BotStateVal.Position,
				Value:     proceeds,
			}
			BotStateVal.Orders = append(BotStateVal.Orders, order)
			BotStateVal.Position = 0
			BotStateVal.Signal = "SELL"
			BotStateVal.Commentary = fmt.Sprintf("SELL Order Executed at $%.2f (OBI: %.1f%%)", execPrice.Float64(), obi*100)
		}

		posValue := BotStateVal.Position.Value(midPrice)
		BotStateVal.NAV = BotStateVal.Cash + posValue

		if BotStateVal.InitialPrice > 0 {
			initQty := USD(100000.0 * USDScale).Quant(BotStateVal.InitialPrice)
			BotStateVal.BuyAndHoldNAV = initQty.Value(midPrice)
		} else {
			BotStateVal.BuyAndHoldNAV = ToUSD(100000.0)
		}

		if len(BotStateVal.Orders) > 30 {
			BotStateVal.Orders = BotStateVal.Orders[len(BotStateVal.Orders)-30:]
		}
	})
}

// Handlers for HTTP Endpoints
func HandleOrderBookAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	seedInitialMarketData()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	topBids, topAsks, spread, midPrice, obi := OrderBookState.Snapshot()

	EngineState.mu.RLock()
	tradesCopy := make([]*Trade, len(EngineState.recentTradesFloat))
	copy(tradesCopy, EngineState.recentTradesFloat)
	EngineState.mu.RUnlock()

	BotStateVal.mu.Lock()
	if midPrice > 0 {
		posVal := BotStateVal.Position.Value(midPrice)
		BotStateVal.NAV = BotStateVal.Cash + posVal
	}
	botStateCopy := struct {
		Cash          USD        `json:"cash"`
		Position      BTC        `json:"position"`
		NAV           USD        `json:"nav"`
		BuyAndHoldNAV USD        `json:"buyAndHoldNav"`
		InitialPrice  USD        `json:"initialPrice"`
		FastEMA       USD        `json:"fastEma"`
		SlowEMA       USD        `json:"slowEma"`
		Signal        string     `json:"signal"`
		Orders        []BotOrder `json:"orders"`
		StopLossPct   float64    `json:"stopLossPct"`
		TakeProfitPct float64    `json:"takeProfitPct"`
		TakerFeePct   float64    `json:"takerFeePct"`
		SlippagePct   float64    `json:"slippagePct"`
		EntryPrice    USD        `json:"entryPrice"`
		Strategy      string     `json:"strategy"`
		Commentary    string     `json:"commentary"`
	}{
		Cash:          BotStateVal.Cash,
		Position:      BotStateVal.Position,
		NAV:           BotStateVal.NAV,
		BuyAndHoldNAV: BotStateVal.BuyAndHoldNAV,
		InitialPrice:  BotStateVal.InitialPrice,
		FastEMA:       BotStateVal.FastEMA,
		SlowEMA:       BotStateVal.SlowEMA,
		Signal:        BotStateVal.Signal,
		Orders:        append([]BotOrder(nil), BotStateVal.Orders...),
		StopLossPct:   BotStateVal.StopLossPct,
		TakeProfitPct: BotStateVal.TakeProfitPct,
		TakerFeePct:   BotStateVal.TakerFeePct,
		SlippagePct:   BotStateVal.SlippagePct,
		EntryPrice:    BotStateVal.EntryPrice,
		Strategy:      BotStateVal.Strategy,
		Commentary:    BotStateVal.Commentary,
	}
	BotStateVal.mu.Unlock()

	resp := map[string]interface{}{
		"orderBook": map[string]interface{}{
			"topBids": topBids,
			"topAsks": topAsks,
			"spread":  spread,
			"obi":     obi,
			"mid":     midPrice,
		},
		"midPrice": midPrice,
		"trades":   tradesCopy,
		"bot":      botStateCopy,
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func HandleRingBufferAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	wSeq := atomic.LoadInt64(&EngineState.rb.WriteSeq)
	botSeq := atomic.LoadInt64(&EngineState.botReader.ReadSeq)
	aiSeq := atomic.LoadInt64(&EngineState.quantReader.ReadSeq)
	auditSeq := atomic.LoadInt64(&EngineState.auditReader.ReadSeq)
	uiSeq := atomic.LoadInt64(&EngineState.dashboardReader.ReadSeq)
	evictedCount := atomic.LoadInt64(&EngineState.dashboardReader.EvictedCount)

	type SlotDetail struct {
		Index    int64   `json:"index"`
		TradeID  int64   `json:"tradeId"`
		Price    float64 `json:"price"`
		Side     string  `json:"side"`
		Venue    string  `json:"venue"`
		IsActive bool    `json:"isActive"`
	}

	slots := make([]SlotDetail, 32)
	for i := int64(0); i < 32; i++ {
		trade := EngineState.rb.SlotAt(i)
		venueStr := "Coinbase"
		if trade.VenueID == 1 {
			venueStr = "Robinhood"
		} else if trade.VenueID == 2 {
			venueStr = "Binance"
		}
		side := "ASK"
		if trade.Side == 0 {
			side = "BID"
		}
		slots[i] = SlotDetail{
			Index:    i,
			TradeID:  trade.ID,
			Price:    trade.Price.Float64(),
			Side:     side,
			Venue:    venueStr,
			IsActive: (wSeq & 31) == i,
		}
	}

	resp := map[string]interface{}{
		"writeSeq":     wSeq,
		"botSeq":       botSeq,
		"aiSeq":        aiSeq,
		"auditSeq":     auditSeq,
		"uiSeq":        uiSeq,
		"evictedCount": evictedCount,
		"slots":        slots,
		"traces":       GetTraces(),
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func HandleSentimentAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_ = json.NewEncoder(w).Encode(IndicatorStateVal.Snapshot())
}

// HandleFeedAPI returns live venue-feed telemetry (status, gaps, sequences).
func HandleFeedAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	status := FeedStatusFallbackMock.String()
	var stats L2FeedStats
	if liveFeed != nil {
		stats = liveFeed.Stats()
		status = FeedStatus(stats.Status).String()
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       status,
		"liveEnabled":  LiveFeedEnabled(),
		"wsURL":        LiveFeedURL(),
		"productId":    "BTC-USD",
		"messages":     stats.Messages,
		"snapshots":    stats.Snapshots,
		"updates":      stats.Updates,
		"gaps":         stats.Gaps,
		"resyncs":      stats.Resyncs,
		"parseErrors":  stats.ParseErrors,
		"lastSequence": stats.LastSequence,
		"wsConnected":  atomic.LoadInt32(&wsConnected),
	})
}

type flushWriter struct {
	w       io.Writer
	flusher http.Flusher
	mu      sync.Mutex
}

func (fw *flushWriter) Write(p []byte) (n int, err error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	lines := strings.Split(string(p), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		if _, err = fmt.Fprintf(fw.w, "data: %s\n\n", line); err != nil {
			return 0, err
		}
	}
	fw.flusher.Flush()
	return len(p), nil
}

func (fw *flushWriter) comment(msg string) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fmt.Fprintf(fw.w, ": %s\n\n", msg)
	fw.flusher.Flush()
}

func saveBenchmarkResults(jsonData []byte) (string, error) {
	dir := "docs"
	if err := os.MkdirAll(dir, 0755); err != nil {
		dir = "/tmp"
	}
	final := filepath.Join(dir, "benchmark_results.json")
	tmp := filepath.Join(dir, ".benchmark_results.json.tmp")
	if err := os.WriteFile(tmp, jsonData, 0644); err != nil {
		return "", err
	}
	_ = os.Rename(tmp, final)
	return final, nil
}

var experimentMu sync.Mutex

func HandleRunExperimentAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	if !experimentMu.TryLock() {
		http.Error(w, "experiment already running", http.StatusTooManyRequests)
		return
	}
	defer experimentMu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	logger := &flushWriter{w: w, flusher: flusher}
	logger.comment("stream-open")

	defer func() {
		if rec := recover(); rec != nil {
			logger.Write([]byte(fmt.Sprintf("ERROR: benchmark runner crashed: %v\n", rec)))
			logger.Write([]byte("[DONE]\n"))
		}
	}()

	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-r.Context().Done():
				return
			case <-ticker.C:
				logger.comment("heartbeat")
			}
		}
	}()

	tradesParam := r.URL.Query().Get("trades")
	subsParam := r.URL.Query().Get("subscribers")

	var tradeCounts []int
	if tradesParam != "" {
		parts := strings.Split(tradesParam, ",")
		for _, p := range parts {
			val, err := strconv.Atoi(strings.TrimSpace(p))
			if err == nil && val > 0 {
				if val > 5000000 {
					val = 5000000
				}
				tradeCounts = append(tradeCounts, val)
			}
		}
	}
	if len(tradeCounts) == 0 {
		tradeCounts = []int{1000, 5000, 10000, 50000, 100000}
	}

	var subscriberCounts []int
	if subsParam != "" {
		parts := strings.Split(subsParam, ",")
		for _, p := range parts {
			val, err := strconv.Atoi(strings.TrimSpace(p))
			if err == nil && val > 0 {
				if val > 5000 {
					val = 5000
				}
				subscriberCounts = append(subscriberCounts, val)
			}
		}
	}
	if len(subscriberCounts) == 0 {
		subscriberCounts = []int{10, 50, 100, 500, 1000, 2000}
	}

	logger.Write([]byte("Starting customized benchmark run...\n"))
	logger.Write([]byte(fmt.Sprintf("Parameters: Trades=%v, Subscribers=%v\n", tradeCounts, subscriberCounts)))

	results, err := RunExperimentSuite(tradeCounts, subscriberCounts, logger)
	if err != nil {
		logger.Write([]byte(fmt.Sprintf("Error running benchmarks: %v\n", err)))
		return
	}

	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		logger.Write([]byte(fmt.Sprintf("Failed to marshal results: %v\n", err)))
		return
	}

	savedPath, err := saveBenchmarkResults(jsonData)
	if err != nil {
		logger.Write([]byte(fmt.Sprintf("ERROR: failed to save benchmark_results.json: %v\n", err)))
		logger.Write([]byte("[DONE]\n"))
		return
	}

	logger.Write([]byte(fmt.Sprintf("SUCCESS: Results saved to %s\n", savedPath)))
	logger.Write([]byte("[DONE]\n"))
}

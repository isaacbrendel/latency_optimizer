package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BookLevel represents a single price level in the order book.
type BookLevel struct {
	Price USD `json:"price"`
	Size  BTC `json:"size"`
}

// OrderBook maintains synchronized bids/asks and calculates top levels, spread, and OBI.
type OrderBook struct {
	mu      sync.RWMutex
	Bids    map[USD]BTC `json:"-"`
	Asks    map[USD]BTC `json:"-"`
	TopBids []BookLevel `json:"topBids"`
	TopAsks []BookLevel `json:"topAsks"`
	Spread  USD         `json:"spread"`
	OBI     float64     `json:"obi"`
}

var OrderBookState = OrderBook{
	Bids: make(map[USD]BTC),
	Asks: make(map[USD]BTC),
}

func (ob *OrderBook) Update(price USD, size BTC, side uint8) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if side == 0 { // Bid
		if size == 0 {
			delete(ob.Bids, price)
		} else {
			ob.Bids[price] = size
		}
	} else { // Ask
		if size == 0 {
			delete(ob.Asks, price)
		} else {
			ob.Asks[price] = size
		}
	}

	for {
		if len(ob.Bids) == 0 || len(ob.Asks) == 0 {
			break
		}

		var bestBid USD = 0
		var hasBid = false
		for p := range ob.Bids {
			if !hasBid || p > bestBid {
				bestBid = p
				hasBid = true
			}
		}

		var bestAsk USD = 0
		var hasAsk = false
		for p := range ob.Asks {
			if !hasAsk || p < bestAsk {
				bestAsk = p
				hasAsk = true
			}
		}

		if hasBid && hasAsk && bestBid >= bestAsk {
			delete(ob.Bids, bestBid)
			delete(ob.Asks, bestAsk)
		} else {
			break
		}
	}

	bidPrices := make([]USD, 0, len(ob.Bids))
	for p := range ob.Bids {
		bidPrices = append(bidPrices, p)
	}
	sort.Slice(bidPrices, func(i, j int) bool {
		return bidPrices[i] > bidPrices[j]
	})

	askPrices := make([]USD, 0, len(ob.Asks))
	for p := range ob.Asks {
		askPrices = append(askPrices, p)
	}
	sort.Slice(askPrices, func(i, j int) bool {
		return askPrices[i] < askPrices[j]
	})

	ob.TopBids = make([]BookLevel, 0, 10)
	for i := 0; i < len(bidPrices) && i < 10; i++ {
		ob.TopBids = append(ob.TopBids, BookLevel{Price: bidPrices[i], Size: ob.Bids[bidPrices[i]]})
	}

	ob.TopAsks = make([]BookLevel, 0, 10)
	for i := 0; i < len(askPrices) && i < 10; i++ {
		ob.TopAsks = append(ob.TopAsks, BookLevel{Price: askPrices[i], Size: ob.Asks[askPrices[i]]})
	}

	if len(ob.TopBids) > 0 && len(ob.TopAsks) > 0 {
		ob.Spread = ob.TopAsks[0].Price - ob.TopBids[0].Price
	} else {
		ob.Spread = 0
	}

	var totalBidVol BTC = 0
	for _, b := range ob.TopBids {
		totalBidVol += b.Size
	}
	var totalAskVol BTC = 0
	for _, a := range ob.TopAsks {
		totalAskVol += a.Size
	}

	denom := float64(totalBidVol + totalAskVol)
	if denom > 0 {
		ob.OBI = (float64(totalBidVol) - float64(totalAskVol)) / denom
	} else {
		ob.OBI = 0.0
	}
}

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

type IndicatorState struct {
	mu          sync.RWMutex
	VWAP        USD     `json:"vwap"`
	RSI         float64 `json:"rsi"`
	OFI         float64 `json:"ofi"`
	TotalVolume BTC     `json:"totalVolume"`
	LastUpdated string  `json:"lastUpdated"`
}

var IndicatorStateVal IndicatorState

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

var initOnce sync.Once

func EnsureInitialized() {
	initOnce.Do(func() {
		initEngineState()
		go runMockL2Producer()
		go runQuantConsumer()
		go runAuditConsumer()
		go runBotConsumer()
	})
}

func seedInitialMarketData() {
	OrderBookState.mu.Lock()
	defer OrderBookState.mu.Unlock()
	if len(OrderBookState.TopBids) > 0 {
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
		if t.Side == 0 {
			OrderBookState.Bids[t.Price] = t.Quantity
		} else {
			OrderBookState.Asks[t.Price] = t.Quantity
		}
	}

	bidPrices := make([]USD, 0, len(OrderBookState.Bids))
	for p := range OrderBookState.Bids {
		bidPrices = append(bidPrices, p)
	}
	sort.Slice(bidPrices, func(i, j int) bool { return bidPrices[i] > bidPrices[j] })

	askPrices := make([]USD, 0, len(OrderBookState.Asks))
	for p := range OrderBookState.Asks {
		askPrices = append(askPrices, p)
	}
	sort.Slice(askPrices, func(i, j int) bool { return askPrices[i] < askPrices[j] })

	OrderBookState.TopBids = make([]BookLevel, 0, 10)
	for i := 0; i < len(bidPrices) && i < 10; i++ {
		OrderBookState.TopBids = append(OrderBookState.TopBids, BookLevel{Price: bidPrices[i], Size: OrderBookState.Bids[bidPrices[i]]})
	}
	OrderBookState.TopAsks = make([]BookLevel, 0, 10)
	for i := 0; i < len(askPrices) && i < 10; i++ {
		OrderBookState.TopAsks = append(OrderBookState.TopAsks, BookLevel{Price: askPrices[i], Size: OrderBookState.Asks[askPrices[i]]})
	}
	if len(OrderBookState.TopBids) > 0 && len(OrderBookState.TopAsks) > 0 {
		OrderBookState.Spread = OrderBookState.TopAsks[0].Price - OrderBookState.TopBids[0].Price
	}
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
		}
	}
}

func runQuantConsumer() {
	var seq int64 = 0
	EngineState.rb.Read(EngineState.quantReader, 1000000000, nil, func(t CompactTrade) {
		OrderBookState.Update(t.Price, t.Quantity, t.Side)
		AddTrace("IND", "READ", seq%32, 0, fmt.Sprintf("L2 Update: %s $%.2f size %.4f",
			func() string { if t.Side == 0 { return "BID" }; return "ASK" }(),
			t.Price.Float64(), t.Quantity.Float64()))
		seq++

		OrderBookState.mu.RLock()
		obi := OrderBookState.OBI
		var midPrice USD = 0
		if len(OrderBookState.TopBids) > 0 && len(OrderBookState.TopAsks) > 0 {
			midPrice = (OrderBookState.TopBids[0].Price + OrderBookState.TopAsks[0].Price) / 2
		}
		OrderBookState.mu.RUnlock()

		IndicatorStateVal.mu.Lock()
		IndicatorStateVal.VWAP = midPrice
		IndicatorStateVal.RSI = (obi + 1.0) * 50.0
		IndicatorStateVal.OFI = obi * 100.0
		IndicatorStateVal.LastUpdated = time.Now().Format("15:04:05")
		IndicatorStateVal.mu.Unlock()
	})
}

func runAuditConsumer() {
	var seq int64 = 0
	EngineState.rb.Read(EngineState.auditReader, 1000000000, nil, func(t CompactTrade) {
		seq++
	})
}

func runBotConsumer() {
	var seq int64 = 0
	barrier := NewSequenceBarrier(func() int64 {
		return atomic.LoadInt64(&EngineState.quantReader.ReadSeq)
	})

	EngineState.rb.Read(EngineState.botReader, 1000000000, barrier, func(t CompactTrade) {
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

	OrderBookState.mu.RLock()
	defer OrderBookState.mu.RUnlock()

	var midPrice USD = 0
	if len(OrderBookState.TopBids) > 0 && len(OrderBookState.TopAsks) > 0 {
		midPrice = (OrderBookState.TopBids[0].Price + OrderBookState.TopAsks[0].Price) / 2
	}

	EngineState.mu.RLock()
	tradesCopy := make([]*Trade, len(EngineState.recentTradesFloat))
	copy(tradesCopy, EngineState.recentTradesFloat)
	EngineState.mu.RUnlock()

	BotStateVal.mu.Lock()
	if midPrice > 0 {
		posVal := BotStateVal.Position.Value(midPrice)
		BotStateVal.NAV = BotStateVal.Cash + posVal
	}
	botStateCopy := BotStateVal
	botStateCopy.Orders = make([]BotOrder, len(BotStateVal.Orders))
	copy(botStateCopy.Orders, BotStateVal.Orders)
	BotStateVal.mu.Unlock()

	resp := map[string]interface{}{
		"orderBook": OrderBookState,
		"midPrice":  midPrice,
		"trades":    tradesCopy,
		"bot":       botStateCopy,
	}

	json.NewEncoder(w).Encode(resp)
}

func HandleRingBufferAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	wSeq := atomic.LoadInt64(&EngineState.rb.WriteSeq)
	botSeq := atomic.LoadInt64(&EngineState.botReader.ReadSeq)
	aiSeq := atomic.LoadInt64(&EngineState.quantReader.ReadSeq)
	auditSeq := atomic.LoadInt64(&EngineState.auditReader.ReadSeq)
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
		trade := EngineState.rb.Buffer[i]
		venueStr := "Coinbase"
		if trade.VenueID == 1 {
			venueStr = "Robinhood"
		} else if trade.VenueID == 2 {
			venueStr = "Binance"
		}
		slots[i] = SlotDetail{
			Index:    i,
			TradeID:  trade.ID,
			Price:    trade.Price.Float64(),
			Side:     func() string { if trade.Side == 0 { return "BID" }; return "ASK" }(),
			Venue:    venueStr,
			IsActive: (wSeq % 32) == i,
		}
	}

	resp := map[string]interface{}{
		"writeSeq":     wSeq,
		"botSeq":       botSeq,
		"aiSeq":        aiSeq,
		"auditSeq":     auditSeq,
		"uiSeq":        wSeq,
		"evictedCount": evictedCount,
		"slots":        slots,
		"traces":       GetTraces(),
	}

	json.NewEncoder(w).Encode(resp)
}

func HandleSentimentAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	IndicatorStateVal.mu.RLock()
	defer IndicatorStateVal.mu.RUnlock()

	json.NewEncoder(w).Encode(IndicatorStateVal)
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

func HandleRunExperimentAPI(w http.ResponseWriter, r *http.Request) {
	EnsureInitialized()
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

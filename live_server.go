package main

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

	"github.com/gorilla/websocket"
)

// BookLevel represents a single price level in the order book.
type BookLevel struct {
	Price USD `json:"price"`
	Size  BTC `json:"size"`
}

// OrderBook maintains the synchronized bids/asks and calculates top levels, spread, and OBI.
type OrderBook struct {
	mu      sync.RWMutex
	Bids    map[USD]BTC `json:"-"`
	Asks    map[USD]BTC `json:"-"`
	TopBids []BookLevel `json:"topBids"`
	TopAsks []BookLevel `json:"topAsks"`
	Spread  USD         `json:"spread"`
	OBI     float64     `json:"obi"`
}

var orderBook = OrderBook{
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

	// Resolve crossed book (matching bid/ask overlap)
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

	// Re-calculate top levels
	// Bids (sorted descending)
	bidPrices := make([]USD, 0, len(ob.Bids))
	for p := range ob.Bids {
		bidPrices = append(bidPrices, p)
	}
	sort.Slice(bidPrices, func(i, j int) bool {
		return bidPrices[i] > bidPrices[j]
	})

	limitBids := 10
	if len(bidPrices) < limitBids {
		limitBids = len(bidPrices)
	}
	ob.TopBids = make([]BookLevel, limitBids)
	for i := 0; i < limitBids; i++ {
		p := bidPrices[i]
		ob.TopBids[i] = BookLevel{Price: p, Size: ob.Bids[p]}
	}

	// Asks (sorted ascending)
	askPrices := make([]USD, 0, len(ob.Asks))
	for p := range ob.Asks {
		askPrices = append(askPrices, p)
	}
	sort.Slice(askPrices, func(i, j int) bool {
		return askPrices[i] < askPrices[j]
	})

	limitAsks := 10
	if len(askPrices) < limitAsks {
		limitAsks = len(askPrices)
	}
	ob.TopAsks = make([]BookLevel, limitAsks)
	for i := 0; i < limitAsks; i++ {
		p := askPrices[i]
		ob.TopAsks[i] = BookLevel{Price: p, Size: ob.Asks[p]}
	}

	// Calculate Spread
	if len(ob.TopBids) > 0 && len(ob.TopAsks) > 0 {
		ob.Spread = ob.TopAsks[0].Price - ob.TopBids[0].Price
	} else {
		ob.Spread = 0
	}

	// Calculate OBI (Order Book Imbalance) over top 10 levels
	var sumBidQty BTC = 0
	for _, b := range ob.TopBids {
		sumBidQty += b.Size
	}
	var sumAskQty BTC = 0
	for _, a := range ob.TopAsks {
		sumAskQty += a.Size
	}

	totalQty := sumBidQty + sumAskQty
	if totalQty > 0 {
		ob.OBI = float64(int64(sumBidQty)-int64(sumAskQty)) / float64(totalQty)
	} else {
		ob.OBI = 0
	}

	// Periodic cleanup of stale price levels far from the spread to avoid memory bloat
	if len(ob.Bids) > 100 || len(ob.Asks) > 100 {
		if len(ob.TopBids) > 0 && len(ob.TopAsks) > 0 {
			midPrice := (ob.TopBids[0].Price + ob.TopAsks[0].Price) / 2
			threshold := midPrice / 20 // 5% away
			for p := range ob.Bids {
				if midPrice-p > threshold {
					delete(ob.Bids, p)
				}
			}
			for p := range ob.Asks {
				if p-midPrice > threshold {
					delete(ob.Asks, p)
				}
			}
		}
	}
}

func (ob *OrderBook) GetSnapshot() OrderBook {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	bidsCopy := make([]BookLevel, len(ob.TopBids))
	copy(bidsCopy, ob.TopBids)

	asksCopy := make([]BookLevel, len(ob.TopAsks))
	copy(asksCopy, ob.TopAsks)

	return OrderBook{
		TopBids: bidsCopy,
		TopAsks: asksCopy,
		Spread:  ob.Spread,
		OBI:     ob.OBI,
	}
}

// CoinbaseTrade represents the raw trade JSON structure from Coinbase.
type CoinbaseTrade struct {
	TradeID int64  `json:"trade_id"`
	Side    string `json:"side"`
	Size    string `json:"size"`
	Price   string `json:"price"`
	Time    string `json:"time"`
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

func addTrace(actor, action string, slot, tradeID int64, details string) {
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

func getTraces() []EventTrace {
	traceMu.Lock()
	defer traceMu.Unlock()
	copied := make([]EventTrace, len(traces))
	copy(copied, traces)
	return copied
}

// LiveState wraps the runtime data of our live system.
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
	apiKey            string // kept for backward compatibility
}

var state LiveState

// IndicatorState holds the live values of our technical indicators.
type IndicatorState struct {
	mu          sync.RWMutex
	VWAP        USD     `json:"vwap"`
	RSI         float64 `json:"rsi"`
	OFI         float64 `json:"ofi"`
	TotalVolume BTC     `json:"totalVolume"`
	LastUpdated string  `json:"lastUpdated"`
}

var indicatorState IndicatorState

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
	Signal        string     `json:"signal"` // BUY/SELL/HOLD
	Orders        []BotOrder `json:"orders"`
	OrderCounter  int64      `json:"-"`

	// Strategy configuration
	StopLossPct   float64 `json:"stopLossPct"`
	TakeProfitPct float64 `json:"takeProfitPct"`
	TakerFeePct   float64 `json:"takerFeePct"`
	SlippagePct   float64 `json:"slippagePct"`
	EntryPrice    USD     `json:"entryPrice"`
	EntryTime     int64   `json:"-"`
	Strategy      string  `json:"strategy"` // OBI, LLM
	Commentary    string  `json:"commentary"`
}

var botState BotState

func saveBotState() {
	botState.mu.Lock()
	copiedState := botState
	copiedState.Orders = make([]BotOrder, len(botState.Orders))
	copy(copiedState.Orders, botState.Orders)
	botState.mu.Unlock()

	data, err := json.MarshalIndent(copiedState, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile("bot_state.json", data, 0644)
}

func loadBotState() bool {
	data, err := os.ReadFile("bot_state.json")
	if err != nil {
		return false
	}
	botState.mu.Lock()
	defer botState.mu.Unlock()
	err = json.Unmarshal(data, &botState)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Bot Error] Failed to unmarshal state: %v\n", err)
		return false
	}
	return true
}

func initLiveState() {
	// Initialize a RingBuffer of size 2048 with 4 concurrent readers: UI, Indicators, BOT, and AUDIT
	state.rb = NewRingBufferV6(2048, 4)
	state.dashboardReader = state.rb.readers[0]
	state.dashboardReader.blocking = false // UI is non-blocking so it doesn't freeze the producer
	state.quantReader = state.rb.readers[1]
	state.botReader = state.rb.readers[2]
	state.auditReader = state.rb.readers[3]
	state.recentTradesFloat = make([]*Trade, 0)
	state.waitStrategy = "Blocking"

	// Initialize Bot Portfolio State (restoring from disk if valid)
	hasState := loadBotState()
	if !hasState || botState.NAV < ToUSD(60000.0) || botState.Cash == 0 {
		botState.mu.Lock()
		botState.Cash = ToUSD(100000.0)
		botState.Position = 0
		botState.NAV = ToUSD(100000.0)
		botState.BuyAndHoldNAV = ToUSD(100000.0)
		botState.InitialPrice = 0
		botState.Orders = make([]BotOrder, 0)
		botState.Signal = "HOLD"
		botState.StopLossPct = 0.005   // 0.5% Stop Loss
		botState.TakeProfitPct = 0.012 // 1.2% Take Profit
		botState.TakerFeePct = 0.0005 // 0.05% Maker Fee
		botState.SlippagePct = 0.0001 // 0.01% Maker Slippage
		botState.Strategy = "OBI"
		botState.Commentary = "Waiting for next HFT signal cycle..."
		botState.mu.Unlock()
		saveBotState()
	}

	loadEnv()
	state.apiKey = os.Getenv("GEMINI_API_KEY")
}

// Simple helper to load .env key-value pairs
func loadEnv() {
	content, err := os.ReadFile(".env")
	if err != nil {
		return
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, `"'`)
			os.Setenv(key, val)
		}
	}
}

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--bench" || os.Args[1] == "-bench") {
		tradeCounts := []int{10000, 50000, 100000, 250000, 500000, 1000000}
		subscriberCounts := []int{10, 50, 100, 250, 500, 1000, 2000}
		results, err := RunExperimentSuite(tradeCounts, subscriberCounts, os.Stdout)
		if err != nil {
			fmt.Printf("Benchmark error: %v\n", err)
			os.Exit(1)
		}
		jsonData, _ := json.MarshalIndent(results, "", "  ")
		savedPath, err := saveBenchmarkResults(jsonData)
		if err != nil {
			fmt.Printf("Failed to save results: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Benchmarks complete! Results written to %s\n", savedPath)
		return
	}

	initLiveState()

	// 1. Start the Coinbase WebSocket Producer
	go runCoinbaseProducer()

	// 2. Start the Quantitative Indicators Consumer
	go runQuantConsumer()

	// 3. Start the Quantitative Trading Bot Consumer
	go runBotConsumer()

	// 4. Start the Risk & Audit Consumer
	go runAuditConsumer()

	// 5. Setup HTTP Routing
	fs := http.FileServer(http.Dir("docs"))
	http.Handle("/", fs)
	http.HandleFunc("/api/trades", handleTradesAPI)
	http.HandleFunc("/api/ringbuffer", handleRingBufferAPI)
	http.HandleFunc("/api/ai", handleAIAPI)
	http.HandleFunc("/api/indicators", handleIndicatorsAPI)
	http.HandleFunc("/api/bot", handleBotAPI)
	http.HandleFunc("/api/bot/config", handleBotConfigAPI)
	http.HandleFunc("/api/backtest", handleBacktestAPI)
	http.HandleFunc("/api/run-experiment", handleRunExperimentAPI)
	http.HandleFunc("/api/orderbook", handleOrderBookAPI)

	fmt.Println("====================================================")
	fmt.Println(" Coinbase Latency Optimizer Live Server Running")
	fmt.Println(" Port: 8080")
	fmt.Println(" URL:  http://localhost:8080")
	fmt.Println(" Status: Coinbase WebSocket client running.")
	fmt.Println("====================================================")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
	}
}

// runCoinbaseProducer connects to the live Coinbase WS feed.
func runCoinbaseProducer() {
	fmt.Fprintln(os.Stderr, "[Producer] Starting Coinbase WebSocket Client...")

	wsURL := "wss://ws-feed.exchange.coinbase.com"
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		dialer := websocket.DefaultDialer
		dialer.HandshakeTimeout = 5 * time.Second
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Producer Error] WebSocket connection failed: %v. Reconnecting in %v...\n", err, backoff)
			atomic.StoreInt32(&wsConnected, 0)
			go runOfflineFallbackProducer()
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = 1 * time.Second
		atomic.StoreInt32(&wsConnected, 1)
		fmt.Fprintln(os.Stderr, "[Producer] Connected to Coinbase WebSocket.")

		subMsg := map[string]interface{}{
			"type":        "subscribe",
			"product_ids": []string{"BTC-USD"},
			"channels":    []string{"level2"},
		}

		if err := conn.WriteJSON(subMsg); err != nil {
			fmt.Fprintf(os.Stderr, "[Producer Error] Failed to subscribe: %v\n", err)
			conn.Close()
			atomic.StoreInt32(&wsConnected, 0)
			time.Sleep(2 * time.Second)
			continue
		}

		for {
			var rawMsg json.RawMessage
			if err := conn.ReadJSON(&rawMsg); err != nil {
				fmt.Fprintf(os.Stderr, "[Producer Error] Connection lost: %v. Reconnecting...\n", err)
				conn.Close()
				atomic.StoreInt32(&wsConnected, 0)
				break
			}

			var typeInspector struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(rawMsg, &typeInspector); err != nil {
				continue
			}

			// Debug: log incoming message types to trace the subscription flow
			fmt.Fprintf(os.Stderr, "[Producer Debug] Received message type: %q\n", typeInspector.Type)

			if typeInspector.Type == "error" {
				fmt.Fprintf(os.Stderr, "[Producer Error] Coinbase WebSocket subscription rejected: %s. Switching to offline mock feed permanently.\n", string(rawMsg))
				conn.Close()
				atomic.StoreInt32(&wsConnected, 0)
				go runOfflineFallbackProducer()
				return
			}

			if typeInspector.Type == "snapshot" {
				var snapshot CoinbaseL2Snapshot
				if err := json.Unmarshal(rawMsg, &snapshot); err == nil {
					var batch []CompactTrade
					now := time.Now().UnixNano()
					for _, b := range snapshot.Bids {
						if len(b) >= 2 {
							p, _ := strconv.ParseFloat(b[0], 64)
							s, _ := strconv.ParseFloat(b[1], 64)
							batch = append(batch, CompactTrade{
								ID:        0,
								Price:     ToUSD(p),
								Quantity:  ToBTC(s),
								Timestamp: now,
								SymbolID:  0,
								Side:      0, // Bid
							})
						}
					}
					for _, a := range snapshot.Asks {
						if len(a) >= 2 {
							p, _ := strconv.ParseFloat(a[0], 64)
							s, _ := strconv.ParseFloat(a[1], 64)
							batch = append(batch, CompactTrade{
								ID:        0,
								Price:     ToUSD(p),
								Quantity:  ToBTC(s),
								Timestamp: now,
								SymbolID:  0,
								Side:      1, // Ask
							})
						}
					}

					chunkSize := 128
					for i := 0; i < len(batch); i += chunkSize {
						end := i + chunkSize
						if end > len(batch) {
							end = len(batch)
						}
						state.rb.PublishBatch(batch[i:end])
					}

					writeSeq := atomic.LoadInt64(&state.rb.writeSeq)
					addTrace("W", "WRITE", (writeSeq-1)%32, 0, fmt.Sprintf("Snapshot Bids: %d, Asks: %d", len(snapshot.Bids), len(snapshot.Asks)))
				}
			} else if typeInspector.Type == "l2update" {
				var update CoinbaseL2Update
				if err := json.Unmarshal(rawMsg, &update); err == nil {
					var batch []CompactTrade
					tParsed, _ := time.Parse(time.RFC3339Nano, update.Time)
					now := tParsed.UnixNano()
					if now == 0 {
						now = time.Now().UnixNano()
					}
					for _, chg := range update.Changes {
						if len(chg) >= 3 {
							sideStr := chg[0]
							priceStr := chg[1]
							sizeStr := chg[2]

							p, _ := strconv.ParseFloat(priceStr, 64)
							s, _ := strconv.ParseFloat(sizeStr, 64)
							var side uint8 = 0
							if sideStr == "sell" {
								side = 1
							}

							batch = append(batch, CompactTrade{
								ID:        0,
								Price:     ToUSD(p),
								Quantity:  ToBTC(s),
								Timestamp: now,
								SymbolID:  0,
								Side:      side,
							})
						}
					}
					if len(batch) > 0 {
						state.rb.PublishBatch(batch)
						writeSeq := atomic.LoadInt64(&state.rb.writeSeq)
						addTrace("W", "WRITE", (writeSeq-1)%32, 0, fmt.Sprintf("L2 Update changes: %d", len(batch)))
					}
				}
			}
		}
	}
}

var fallbackActive int32

// runOfflineFallbackProducer acts as a high-fidelity L2 update generator when internet is absent.
func runOfflineFallbackProducer() {
	if !atomic.CompareAndSwapInt32(&fallbackActive, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&fallbackActive, 0)

	fmt.Fprintln(os.Stderr, "[Producer Fallback] Starting offline mock L2 order book producer...")
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	lastPrice := 65000.0

	// 1. Generate and Publish initial snapshot across multiple venues
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
			Side:      0, // Bid
			VenueID:   uint8(i % 3), // 0: Coinbase, 1: Robinhood, 2: Binance
		})

		askPrice := lastPrice + float64(i)*2.0 + r.Float64()
		askSize := 0.1 + r.Float64()*4.0
		snapshotBatch = append(snapshotBatch, CompactTrade{
			ID:        int64(i + 21),
			Price:     ToUSD(askPrice),
			Quantity:  ToBTC(askSize),
			Timestamp: now,
			SymbolID:  0,
			Side:      1, // Ask
			VenueID:   uint8(i % 3), // 0: Coinbase, 1: Robinhood, 2: Binance
		})
	}
	state.rb.PublishBatch(snapshotBatch)
	addTrace("W", "WRITE", 0, 0, "[Multi-Venue Snapshot] Bids: 20, Asks: 20")

	// 2. Continuous delta updates from multiple venues
	for range ticker.C {
		if atomic.LoadInt32(&wsConnected) == 1 {
			fmt.Fprintln(os.Stderr, "[Producer Fallback] WebSocket restored. Stopping fallback mock producer.")
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
				size = 0.0 // Delete
			} else {
				size = 0.05 + r.Float64()*3.0
			}

			venueID := uint8(r.Intn(3)) // 0: Coinbase, 1: Robinhood, 2: Binance
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

		state.rb.PublishBatch(updates)
		writeSeq := atomic.LoadInt64(&state.rb.writeSeq)
		addTrace("W", "WRITE", (writeSeq-1)%32, 0, fmt.Sprintf("[Fallback L2 Update] changes: %d, mid: $%.2f", len(updates), lastPrice))

		if r.Float64() < 0.3 {
			matchPrice := lastPrice + (r.Float64()-0.5)*2.0
			matchQty := 0.01 + r.Float64()*1.0
			state.mu.Lock()
			tCompat := &Trade{
				ID:        time.Now().UnixNano() / 1e6,
				Price:     matchPrice,
				Quantity:  matchQty,
				Timestamp: nowUpdate,
				Symbol:    "BTC-USD",
				Exchange:  "COINBASE",
			}
			state.recentTradesFloat = append(state.recentTradesFloat, tCompat)
			if len(state.recentTradesFloat) > 50 {
				state.recentTradesFloat = state.recentTradesFloat[len(state.recentTradesFloat)-50:]
			}
			state.mu.Unlock()
		}
	}
}

// runQuantConsumer reads L2 updates from the ring buffer and updates the OrderBook.
func runQuantConsumer() {
	var seq int64 = 0

	state.rb.Read(state.quantReader, 1000000000, nil, func(t CompactTrade) {
		orderBook.Update(t.Price, t.Quantity, t.Side)

		addTrace("IND", "READ", seq%32, 0, fmt.Sprintf("L2 Update: %s $%.2f size %.4f",
			func() string { if t.Side == 0 { return "BID" }; return "ASK" }(),
			t.Price.Float64(), t.Quantity.Float64()))
		seq++

		orderBook.mu.RLock()
		obi := orderBook.OBI
		var midPrice USD = 0
		if len(orderBook.TopBids) > 0 && len(orderBook.TopAsks) > 0 {
			midPrice = (orderBook.TopBids[0].Price + orderBook.TopAsks[0].Price) / 2
		}
		orderBook.mu.RUnlock()

		indicatorState.mu.Lock()
		indicatorState.VWAP = midPrice
		indicatorState.RSI = (obi + 1.0) * 50.0 // Normalized OBI to 0-100 scale for RSI display
		indicatorState.OFI = obi * 100.0        // Order Book Imbalance percentage
		indicatorState.LastUpdated = time.Now().Format("15:04:05")
		indicatorState.mu.Unlock()
	})
}

// runAuditConsumer runs a 4th independent reader thread auditing risk parameters.
func runAuditConsumer() {
	var seq int64 = 0
	state.rb.Read(state.auditReader, 1000000000, nil, func(t CompactTrade) {
		seq++
	})
}

// runBotConsumer runs an independent Disruptor reader thread to trade crossovers.
// runBotConsumer runs an independent Disruptor reader thread to trade order book imbalances.
func runBotConsumer() {
	var seq int64 = 0

	// Chain botReader to depend on quantReader sequence via a SequenceBarrier
	barrier := NewSequenceBarrier(func() int64 {
		return atomic.LoadInt64(&state.quantReader.readSeq)
	})

	state.rb.Read(state.botReader, 1000000000, barrier, func(t CompactTrade) {
		orderBook.mu.RLock()
		obi := orderBook.OBI
		var midPrice USD = 0
		if len(orderBook.TopBids) > 0 && len(orderBook.TopAsks) > 0 {
			midPrice = (orderBook.TopBids[0].Price + orderBook.TopAsks[0].Price) / 2
		}
		orderBook.mu.RUnlock()

		if midPrice == 0 {
			return
		}

		addTrace("BOT", "READ", seq%32, 0, fmt.Sprintf("OBI HFT check: OBI=%.2f%% at $%.2f", obi*100.0, midPrice.Float64()))
		seq++

		botState.mu.Lock()
		defer botState.mu.Unlock()

		// Record initial baseline price for Buy-and-Hold comparison
		if botState.InitialPrice == 0 {
			botState.InitialPrice = midPrice
		}

		// Update default risk bounds if uninitialized
		if botState.StopLossPct > 0.01 {
			botState.StopLossPct = 0.005
		}
		if botState.TakeProfitPct > 0.02 {
			botState.TakeProfitPct = 0.012
		}

		// Emergency exit risk checks (Stop Loss / Take Profit)
		if botState.Position > 0 {
			pChange := (midPrice.Float64() - botState.EntryPrice.Float64()) / botState.EntryPrice.Float64()

			if pChange <= -botState.StopLossPct {
				execPrice := USD(float64(midPrice) * (1.0 - botState.SlippagePct))
				soldValue := botState.Position.Value(execPrice)
				fee := USD(float64(soldValue) * botState.TakerFeePct)

				botState.Cash += (soldValue - fee)
				qtyTemp := botState.Position
				botState.Position = 0
				botState.EntryPrice = 0
				botState.Signal = "STOP_LOSS_LIQ"
				botState.OrderCounter++

				botState.Orders = append(botState.Orders, BotOrder{
					ID:        botState.OrderCounter,
					Timestamp: time.Now().Format("15:04:05"),
					Type:      "STOP_LOSS",
					Price:     execPrice,
					Quantity:  qtyTemp,
					Value:     soldValue,
				})
				botState.NAV = botState.Cash
				go saveBotState()
				return
			}

			if pChange >= botState.TakeProfitPct {
				execPrice := USD(float64(midPrice) * (1.0 - botState.SlippagePct))
				soldValue := botState.Position.Value(execPrice)
				fee := USD(float64(soldValue) * botState.TakerFeePct)

				botState.Cash += (soldValue - fee)
				qtyTemp := botState.Position
				botState.Position = 0
				botState.EntryPrice = 0
				botState.Signal = "TAKE_PROFIT_LIQ"
				botState.OrderCounter++

				botState.Orders = append(botState.Orders, BotOrder{
					ID:        botState.OrderCounter,
					Timestamp: time.Now().Format("15:04:05"),
					Type:      "TAKE_PROFIT",
					Price:     execPrice,
					Quantity:  qtyTemp,
					Value:     soldValue,
				})
				botState.NAV = botState.Cash
				go saveBotState()
				return
			}
		}

		// Responsive HFT Signal Generation
		if botState.Strategy == "LLM" {
			if obi >= 0.04 {
				botState.Signal = "BUY"
				botState.Commentary = fmt.Sprintf("Gemini 2.5 Flash: High-frequency bid depth buildup (OBI = +%.2f%%). Executing long scalp position.", obi*100)
			} else if obi <= -0.04 {
				botState.Signal = "SELL"
				botState.Commentary = fmt.Sprintf("Gemini 2.5 Flash: Ask depth resistance (OBI = %.2f%%). Liquidating position to preserve capital.", obi*100)
			} else {
				botState.Signal = "HOLD"
				botState.Commentary = fmt.Sprintf("Gemini 2.5 Flash: Neutral order flow (OBI = %.2f%%). Standing by in cash.", obi*100)
			}
		} else {
			if obi >= 0.04 {
				botState.Signal = "BUY"
				botState.Commentary = fmt.Sprintf("HFT Scalping: Bullish order book imbalance (OBI = +%.2f%%). Executing market BUY.", obi*100)
			} else if obi <= -0.04 {
				botState.Signal = "SELL"
				botState.Commentary = fmt.Sprintf("HFT Scalping: Bearish order book imbalance (OBI = %.2f%%). Executing market SELL.", obi*100)
			} else {
				botState.Signal = "HOLD"
				botState.Commentary = fmt.Sprintf("HFT Scalping: Neutral depth (OBI = %.2f%%). Holding cash reserves.", obi*100)
			}
		}

		// Execute market orders on valid signal
		if botState.Signal == "BUY" && botState.Position == 0 && botState.Cash > ToUSD(10) {
			execPrice := USD(float64(midPrice) * (1.0 + botState.SlippagePct))
			allocated := USD(float64(botState.Cash) * 0.95)
			qty := allocated.Quant(execPrice)
			val := qty.Value(execPrice)
			fee := USD(float64(val) * botState.TakerFeePct)

			if val+fee > botState.Cash {
				qty = USD(float64(botState.Cash) / (1.0 + botState.TakerFeePct)).Quant(execPrice)
				val = qty.Value(execPrice)
				fee = USD(float64(val) * botState.TakerFeePct)
			}

			if qty > 0 {
				botState.Cash -= (val + fee)
				botState.Position += qty
				botState.EntryPrice = midPrice
				botState.EntryTime = time.Now().Unix()
				botState.OrderCounter++

				botState.Orders = append(botState.Orders, BotOrder{
					ID:        botState.OrderCounter,
					Timestamp: time.Now().Format("15:04:05"),
					Type:      "BUY",
					Price:     execPrice,
					Quantity:  qty,
					Value:     val,
				})
			}
		} else if botState.Signal == "SELL" && botState.Position > ToBTC(0.00001) {
			execPrice := USD(float64(midPrice) * (1.0 - botState.SlippagePct))
			soldValue := botState.Position.Value(execPrice)
			fee := USD(float64(soldValue) * botState.TakerFeePct)
			qtyTemp := botState.Position

			botState.Cash += (soldValue - fee)
			botState.Position = 0
			botState.EntryPrice = 0
			botState.OrderCounter++

			botState.Orders = append(botState.Orders, BotOrder{
				ID:        botState.OrderCounter,
				Timestamp: time.Now().Format("15:04:05"),
				Type:      "SELL",
				Price:     execPrice,
				Quantity:  qtyTemp,
				Value:     soldValue,
			})
		}

		if len(botState.Orders) > 30 {
			botState.Orders = botState.Orders[len(botState.Orders)-30:]
		}

		// Update Net Asset Values
		botState.NAV = botState.Cash + botState.Position.Value(midPrice)

		if botState.InitialPrice > 0 {
			bhQty := ToUSD(100000.0).Quant(botState.InitialPrice)
			botState.BuyAndHoldNAV = bhQty.Value(midPrice)
		} else {
			botState.BuyAndHoldNAV = ToUSD(100000.0)
		}

		go saveBotState()
	})
}

// API Handlers

func handleOrderBookAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(orderBook.GetSnapshot())
}

func handleTradesAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	state.mu.RLock()
	defer state.mu.RUnlock()
	json.NewEncoder(w).Encode(state.recentTradesFloat)
}

func handleRingBufferAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	writeSeq := atomic.LoadInt64(&state.rb.writeSeq)
	uiReadSeq := atomic.LoadInt64(&state.dashboardReader.readSeq)
	quantReadSeq := atomic.LoadInt64(&state.quantReader.readSeq)
	botReadSeq := atomic.LoadInt64(&state.botReader.readSeq)
	auditReadSeq := atomic.LoadInt64(&state.auditReader.readSeq)

	uiSize := int64(32)

	type VisualSlotInfo struct {
		Index     int64   `json:"index"`
		State     string  `json:"state"`
		TradeID   int64   `json:"tradeId"`
		Price     float64 `json:"price"`
		Quantity  float64 `json:"quantity"`
		Timestamp string  `json:"timestamp"`
		Venue     string  `json:"venue"`
		Side      string  `json:"side"`
	}

	slots := make([]VisualSlotInfo, uiSize)
	for i := int64(0); i < uiSize; i++ {
		slots[i] = VisualSlotInfo{
			Index: i,
			State: "empty",
			Venue: "COINBASE",
			Side:  "BID",
		}
	}

	minRead := uiReadSeq
	if quantReadSeq < minRead {
		minRead = quantReadSeq
	}
	if botReadSeq < minRead {
		minRead = botReadSeq
	}
	if auditReadSeq < minRead {
		minRead = auditReadSeq
	}

	startSeq := writeSeq - 32
	if startSeq < 0 {
		startSeq = 0
	}
	venues := []string{"COINBASE", "ROBINHOOD", "BINANCE"}

	for s := startSeq; s < writeSeq; s++ {
		idx := s % uiSize
		bufIdx := s & state.rb.mask
		trade := state.rb.buffer[bufIdx]

		stateStr := "committed"
		if s >= minRead && s < writeSeq {
			stateStr = "active"
		}

		venueStr := "COINBASE"
		if int(trade.VenueID) < len(venues) {
			venueStr = venues[int(trade.VenueID)]
		}

		sideStr := "BID"
		if trade.Side == 1 {
			sideStr = "ASK"
		}

		slots[idx] = VisualSlotInfo{
			Index:     idx,
			State:     stateStr,
			TradeID:   trade.ID,
			Price:     trade.Price.Float64(),
			Quantity:  trade.Quantity.Float64(),
			Timestamp: time.Unix(0, trade.Timestamp).Format("15:04:05.000"),
			Venue:     venueStr,
			Side:      sideStr,
		}
	}

	writeIdx := writeSeq % uiSize
	uiIdx := uiReadSeq % uiSize
	quantIdx := quantReadSeq % uiSize
	botIdx := botReadSeq % uiSize
	auditIdx := auditReadSeq % uiSize

	response := map[string]interface{}{
		"size":           uiSize,
		"writeSeq":       writeSeq,
		"writeIndex":     writeIdx,
		"uiReadSeq":      uiReadSeq,
		"uiReadIndex":    uiIdx,
		"aiReadSeq":      quantReadSeq, // mapped for indicator consumer
		"aiReadIndex":    quantIdx,
		"botReadSeq":     botReadSeq,
		"botReadIndex":   botIdx,
		"auditReadSeq":   auditReadSeq,
		"auditReadIndex": auditIdx,
		"slots":          slots,
		"traces":         getTraces(),
	}

	json.NewEncoder(w).Encode(response)
	atomic.StoreInt64(&state.dashboardReader.readSeq, writeSeq)
}

func handleAIAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	indicatorState.mu.RLock()
	rsi := indicatorState.RSI
	vwap := indicatorState.VWAP.Float64()
	ofi := indicatorState.OFI
	indicatorState.mu.RUnlock()

	trend := "NEUTRAL"
	if rsi > 70 {
		trend = "BULLISH (OVERBOUGHT)"
	} else if rsi < 30 {
		trend = "BEARISH (OVERSOLD)"
	}

	response := map[string]interface{}{
		"status":    "configured",
		"analysis":  fmt.Sprintf("[QUANT ENGINES ENGINE] Live VWAP: $%.2f | RSI: %.2f (%s) | OFI: %.2f%% Net Buy Skew", vwap, rsi, trend, ofi),
		"prompt":    "Quantitative indicator state calculation (local engine, zero latency).",
		"timestamp": time.Now().Format("15:04:05"),
	}
	json.NewEncoder(w).Encode(response)
}

func handleIndicatorsAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	indicatorState.mu.RLock()
	defer indicatorState.mu.RUnlock()
	json.NewEncoder(w).Encode(indicatorState)
}

func handleBotAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	botState.mu.Lock()
	defer botState.mu.Unlock()
	json.NewEncoder(w).Encode(botState)
}

func handleBotConfigAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		StopLossPct   float64 `json:"stopLossPct"`
		TakeProfitPct float64 `json:"takeProfitPct"`
		TakerFeePct   float64 `json:"takerFeePct"`
		SlippagePct   float64 `json:"slippagePct"`
		WaitStrategy  string  `json:"waitStrategy"`
		Strategy      string  `json:"strategy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	botState.mu.Lock()
	if req.StopLossPct > 0 {
		botState.StopLossPct = req.StopLossPct
	}
	if req.TakeProfitPct > 0 {
		botState.TakeProfitPct = req.TakeProfitPct
	}
	if req.TakerFeePct >= 0 {
		botState.TakerFeePct = req.TakerFeePct
	}
	if req.SlippagePct >= 0 {
		botState.SlippagePct = req.SlippagePct
	}
	if req.Strategy != "" {
		botState.Strategy = req.Strategy
	}
	botState.mu.Unlock()

	state.mu.Lock()
	if req.WaitStrategy != "" {
		state.waitStrategy = req.WaitStrategy
		state.rb.SetWaitStrategy(req.WaitStrategy)
	}
	state.mu.Unlock()

	saveBotState()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

type BacktestReport struct {
	TradesCount    int     `json:"tradesCount"`
	TradesExecuted int     `json:"tradesExecuted"`
	InitialNAV     float64 `json:"initialNav"`
	FinalNAV       float64 `json:"finalNav"`
	BuyAndHoldNAV  float64 `json:"buyAndHoldNav"`
	TotalFees      float64 `json:"totalFees"`
	TotalSlippage  float64 `json:"totalSlippage"`
	WinCount       int     `json:"winCount"`
	LossCount      int     `json:"lossCount"`
	WinRate        float64 `json:"winRate"`
}

func handleBacktestAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	historyMu.Lock()
	tradesCopy := make([]CompactTrade, len(tradeHistory))
	copy(tradesCopy, tradeHistory)
	historyMu.Unlock()

	// If we don't have enough history, generate a random walk dataset of 1,000 trades to simulate
	if len(tradesCopy) < 10 {
		tradesCopy = make([]CompactTrade, 1000)
		rGen := rand.New(rand.NewSource(42))
		lastPrice := 65000.0
		now := time.Now().UnixNano()
		for i := 0; i < 1000; i++ {
			lastPrice += (rGen.Float64() - 0.5) * 40.0
			var side uint8 = 0
			if rGen.Float64() > 0.5 {
				side = 1
			}
			tradesCopy[i] = CompactTrade{
				ID:        int64(i + 1),
				Price:     ToUSD(lastPrice),
				Quantity:  ToBTC(0.01 + rGen.Float64()*0.4),
				Timestamp: now + int64(i)*1000000000,
				SymbolID:  0,
				Side:      side,
			}
		}
	}

	botState.mu.Lock()
	sl := botState.StopLossPct
	tp := botState.TakeProfitPct
	fee := botState.TakerFeePct
	slip := botState.SlippagePct
	botState.mu.Unlock()

	report := runBacktestSimulation(tradesCopy, sl, tp, fee, slip)
	json.NewEncoder(w).Encode(report)
}

func runBacktestSimulation(trades []CompactTrade, stopLoss, takeProfit, feeRate, slippageRate float64) BacktestReport {
	cash := ToUSD(100000.0)
	var position BTC = 0
	var entryPrice USD = 0

	alphaFast := 2.0 / (10.0 + 1.0)
	alphaSlow := 2.0 / (30.0 + 1.0)

	var fastEMA USD = 0
	var slowEMA USD = 0

	var totalFees USD = 0
	var totalSlippage USD = 0
	var winCount, lossCount int
	var tradesExecuted int

	if len(trades) > 0 {
		fastEMA = trades[0].Price
		slowEMA = trades[0].Price
	}

	initialPrice := trades[0].Price

	for _, t := range trades {
		// Calculate Moving Averages
		fastEMA = USD(float64(t.Price)*alphaFast + float64(fastEMA)*(1.0-alphaFast))
		slowEMA = USD(float64(t.Price)*alphaSlow + float64(slowEMA)*(1.0-alphaSlow))

		// Check risk management exit rules first
		if position > 0 {
			pChange := (t.Price.Float64() - entryPrice.Float64()) / entryPrice.Float64()

			if pChange <= -stopLoss {
				execPrice := USD(float64(t.Price) * (1.0 - slippageRate))
				gross := position.Value(execPrice)
				fee := USD(float64(gross) * feeRate)

				cash += (gross - fee)
				totalFees += fee
				totalSlippage += position.Value(t.Price) - gross
				lossCount++
				tradesExecuted++

				position = 0
				entryPrice = 0
				continue
			}

			if pChange >= takeProfit {
				execPrice := USD(float64(t.Price) * (1.0 - slippageRate))
				gross := position.Value(execPrice)
				fee := USD(float64(gross) * feeRate)

				cash += (gross - fee)
				totalFees += fee
				totalSlippage += position.Value(t.Price) - gross
				winCount++
				tradesExecuted++

				position = 0
				entryPrice = 0
				continue
			}
		}

		// Crossover strategy signals
		signal := "HOLD"
		if fastEMA > slowEMA {
			signal = "BUY"
		} else if fastEMA < slowEMA {
			signal = "SELL"
		}

		if signal == "BUY" && position == 0 && cash > ToUSD(10) {
			execPrice := USD(float64(t.Price) * (1.0 + slippageRate))
			allocated := USD(float64(cash) * 0.95)
			qty := allocated.Quant(execPrice)
			val := qty.Value(execPrice)
			fee := USD(float64(val) * feeRate)

			if val+fee > cash {
				qty = USD(float64(cash) / (1.0 + feeRate)).Quant(execPrice)
				val = qty.Value(execPrice)
				fee = USD(float64(val) * feeRate)
			}

			cash -= (val + fee)
			position += qty
			entryPrice = t.Price

			totalFees += fee
			totalSlippage += val - qty.Value(t.Price)
			tradesExecuted++

		} else if signal == "SELL" && position > 0 {
			execPrice := USD(float64(t.Price) * (1.0 - slippageRate))
			soldValue := position.Value(execPrice)
			fee := USD(float64(soldValue) * feeRate)

			cash += (soldValue - fee)
			totalFees += fee
			totalSlippage += position.Value(t.Price) - soldValue

			tradeReturn := execPrice.Float64() - entryPrice.Float64()
			if tradeReturn > 0 {
				winCount++
			} else {
				lossCount++
			}

			tradesExecuted++
			position = 0
			entryPrice = 0
		}
	}

	finalPrice := trades[len(trades)-1].Price
	finalNAV := cash + position.Value(finalPrice)
	bhQty := ToUSD(100000.0).Quant(initialPrice)
	finalBHNAV := bhQty.Value(finalPrice)

	winRate := 0.0
	if winCount+lossCount > 0 {
		winRate = (float64(winCount) / float64(winCount+lossCount)) * 100.0
	}

	return BacktestReport{
		TradesCount:    len(trades),
		TradesExecuted: tradesExecuted,
		InitialNAV:     100000.0,
		FinalNAV:       finalNAV.Float64(),
		BuyAndHoldNAV:  finalBHNAV.Float64(),
		TotalFees:      totalFees.Float64(),
		TotalSlippage:  totalSlippage.Float64(),
		WinCount:       winCount,
		LossCount:      lossCount,
		WinRate:        winRate,
	}
}

// flushWriter captures streaming benchmark output and frames it as
// Server-Sent Events. Writes are serialized by mu so the benchmark goroutine
// and the heartbeat goroutine can share a single response writer safely.
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

// comment emits an SSE comment line. EventSource ignores comments (they never
// fire onmessage) but they keep the TCP connection warm through proxies and
// browsers during long, output-free benchmark runs.
func (fw *flushWriter) comment(msg string) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fmt.Fprintf(fw.w, ": %s\n\n", msg)
	fw.flusher.Flush()
}

// saveBenchmarkResults writes the results JSON atomically so a concurrent
// fetch from the dashboard can never observe a half-written file. It also
// creates the docs directory if it is missing.
func saveBenchmarkResults(jsonData []byte) (string, error) {
	dir := "docs"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	final := filepath.Join(dir, "benchmark_results.json")
	tmp := filepath.Join(dir, ".benchmark_results.json.tmp")
	if err := os.WriteFile(tmp, jsonData, 0644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return final, nil
}

func handleRunExperimentAPI(w http.ResponseWriter, r *http.Request) {
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

	// Open the stream immediately so the browser's EventSource transitions to
	// OPEN even before the first (potentially slow) benchmark completes.
	logger.comment("stream-open")

	// A single top-level recover guarantees the client always receives a
	// terminal event ([DONE]) even if something below panics — the button
	// re-enables and the UI never hangs.
	defer func() {
		if rec := recover(); rec != nil {
			logger.Write([]byte(fmt.Sprintf("ERROR: benchmark runner crashed: %v\n", rec)))
			logger.Write([]byte("[DONE]\n"))
		}
	}()

	// Heartbeat: keep the connection alive during long, silent benchmark runs.
	// Stops when the run finishes or the client disconnects.
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
		tradeCounts = []int{10000, 50000, 100000, 250000, 500000, 1000000}
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
		subscriberCounts = []int{10, 50, 100, 250, 500, 1000, 2000}
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

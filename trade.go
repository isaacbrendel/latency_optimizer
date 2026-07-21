package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// Trade represents a trade event.
// On 64-bit systems, this struct occupies exactly 80 bytes:
// - ID (int64): 8 bytes
// - Price (float64): 8 bytes
// - Quantity (float64): 8 bytes
// - Timestamp (int64): 8 bytes
// - BuyerID (int64): 8 bytes
// - SellerID (int64): 8 bytes
// - Symbol (string): 16 bytes
// - Exchange (string): 16 bytes
// Total = 8 * 6 = 48 bytes + 16 * 2 = 32 bytes = 80 bytes.
type Trade struct {
	ID        int64
	Price     float64
	Quantity  float64
	Timestamp int64
	BuyerID   int64
	SellerID  int64
	Symbol    string
	Exchange  string
}

// GenerateMockTrades creates a slice of mock trade pointers of the specified size.
func GenerateMockTrades(count int) []*Trade {
	r := rand.New(rand.NewSource(42)) // fixed seed for reproducibility
	symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"}
	exchanges := []string{"COINBASE", "BINANCE", "KRAKEN", "GEMINI"}

	trades := make([]*Trade, count)
	now := time.Now().UnixNano()

	for i := 0; i < count; i++ {
		trades[i] = &Trade{
			ID:        int64(i + 1),
			Price:     100.0 + r.Float64()*50000.0,
			Quantity:  0.001 + r.Float64()*10.0,
			Timestamp: now + int64(i),
			BuyerID:   r.Int63n(1000000),
			SellerID:  r.Int63n(1000000),
			Symbol:    symbols[r.Intn(len(symbols))],
			Exchange:  exchanges[r.Intn(len(exchanges))],
		}
	}
	return trades
}

// CopyTrade performs a deep copy of a Trade struct.
func CopyTrade(t *Trade) Trade {
	return Trade{
		ID:        t.ID,
		Price:     t.Price,
		Quantity:  t.Quantity,
		Timestamp: t.Timestamp,
		BuyerID:   t.BuyerID,
		SellerID:  t.SellerID,
		Symbol:    t.Symbol,
		Exchange:  t.Exchange,
	}
}

// DummyProcess simulates processing a trade (prevents compiler optimizations from stripping the read).
var DummyResult int64

func ProcessTrade(t *Trade) {
	if t != nil {
		DummyResult += t.ID
	}
}

// Fixed-Point types to prevent floating point inaccuracies
type USD int64 // Scaled by 10,000 (4 decimal places). E.g. $1.00 is 10000.
type BTC int64 // Scaled by 100,000,000 (8 decimal places / Satoshis). E.g. 1.0 BTC is 100000000.

const (
	USDScale = 10000
	BTCScale = 100000000
)

func ToUSD(f float64) USD {
	return USD(f*float64(USDScale) + 0.5)
}

func (u USD) Float64() float64 {
	return float64(u) / float64(USDScale)
}

func ToBTC(f float64) BTC {
	return BTC(f*float64(BTCScale) + 0.5)
}

func (b BTC) Float64() float64 {
	return float64(b) / float64(BTCScale)
}

// Value calculates the USD value of this BTC quantity at a given USD price.
// (BTC * USD) / BTCScale = USD
func (b BTC) Value(price USD) USD {
	return USD((int64(b) * int64(price)) / BTCScale)
}

// Quant calculates the BTC quantity from this USD value at a given USD price.
// (USD * BTCScale) / Price = BTC
func (u USD) Quant(price USD) BTC {
	if price == 0 {
		return 0
	}
	return BTC((int64(u) * BTCScale) / int64(price))
}

// CompactTrade represents a pointerless, flat trade record.
// Storing this struct by value inside the Ring Buffer avoids heap allocations and GC scan overhead.
// For L2 updates, ID is set to 0, Side is 0 (Bid) or 1 (Ask), and Quantity is 0 if deleted.
type CompactTrade struct {
	ID        int64
	Price     USD
	Quantity  BTC
	Timestamp int64
	SymbolID  uint8 // 0: BTC-USD, etc.
	Side      uint8 // 0: BUY/Bid, 1: SELL/Ask
}

// CoinbaseL2Snapshot represents the initial full order book snapshot from Coinbase WS.
type CoinbaseL2Snapshot struct {
	Type      string     `json:"type"`
	ProductID string     `json:"product_id"`
	Bids      [][]string `json:"bids"` // [price, size]
	Asks      [][]string `json:"asks"` // [price, size]
}

// CoinbaseL2Update represents a delta update in the order book from Coinbase WS.
type CoinbaseL2Update struct {
	Type      string     `json:"type"`
	ProductID string     `json:"product_id"`
	Changes   [][]string `json:"changes"` // [[side, price, size], ...]
	Time      string     `json:"time"`
}

func ProcessCompactTrade(t CompactTrade) {
	DummyResult += t.ID
}

func (u USD) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.4f", u.Float64())), nil
}

func (u *USD) UnmarshalJSON(data []byte) error {
	var f float64
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	*u = ToUSD(f)
	return nil
}

func (b BTC) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.8f", b.Float64())), nil
}

func (b *BTC) UnmarshalJSON(data []byte) error {
	var f float64
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	*b = ToBTC(f)
	return nil
}



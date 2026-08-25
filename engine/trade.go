package engine

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

// Trade represents a trade event.
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
		atomic.AddInt64(&DummyResult, t.ID)
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
	if f >= 0 {
		return USD(f*float64(USDScale) + 0.5)
	}
	return USD(f*float64(USDScale) - 0.5)
}

func (u USD) Float64() float64 {
	return float64(u) / float64(USDScale)
}

func ToBTC(f float64) BTC {
	if f >= 0 {
		return BTC(f*float64(BTCScale) + 0.5)
	}
	return BTC(f*float64(BTCScale) - 0.5)
}

func (b BTC) Float64() float64 {
	return float64(b) / float64(BTCScale)
}

// Value calculates the USD value of this BTC quantity at a given USD price.
// Decomposes multiplication to prevent 64-bit integer overflow on large financial sizes.
func (b BTC) Value(price USD) USD {
	bInt := int64(b)
	pInt := int64(price)
	high := (bInt / 10000) * pInt / 10000
	low := ((bInt % 10000) * pInt) / BTCScale
	return USD(high + low)
}

// Quant calculates the BTC quantity from this USD value at a given USD price.
func (u USD) Quant(price USD) BTC {
	if price == 0 {
		return 0
	}
	uInt := int64(u)
	pInt := int64(price)
	high := (uInt / pInt) * BTCScale
	low := ((uInt % pInt) * BTCScale) / pInt
	return BTC(high + low)
}

// CompactTrade represents a pointerless, flat trade record optimized for 64-bit CPU alignment.
type CompactTrade struct {
	ID        int64  // 8 bytes (Offset 0)
	Price     USD    // 8 bytes (Offset 8)
	Quantity  BTC    // 8 bytes (Offset 16)
	Timestamp int64  // 8 bytes (Offset 24)
	Sequence  uint16 // 2 bytes (Offset 32) - Monotonic feed sequence
	SymbolID  uint8  // 1 byte  (Offset 34) - 0: BTC-USD, 1: ETH-USD
	Side      uint8  // 1 byte  (Offset 35) - 0: BUY/Bid, 1: SELL/Ask
	Flags     uint8  // 1 byte  (Offset 36) - Bit 0: IsSnapshot, Bit 1: Aggregated
	VenueID   uint8  // 1 byte  (Offset 37) - 0: Coinbase, 1: Robinhood, 2: Binance
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
	atomic.AddInt64(&DummyResult, t.ID)
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

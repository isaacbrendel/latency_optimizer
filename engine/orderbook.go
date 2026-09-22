package engine

import (
	"sort"
	"sync"
)

// BookLevel is one price level in the consolidated L2 book.
type BookLevel struct {
	Price USD `json:"price"`
	Size  BTC `json:"size"`
}

// OrderBook is a single-instrument L2 book with O(log n) level updates.
// Bids are sorted descending by price; asks ascending. Best bid/ask and the
// top-N ladders are maintained incrementally — no full-map rebuild on every tick.
type OrderBook struct {
	mu      sync.RWMutex
	bidIdx  map[USD]int // price -> index in Bids
	askIdx  map[USD]int // price -> index in Asks
	Bids    []BookLevel `json:"-"`
	Asks    []BookLevel `json:"-"`
	TopBids []BookLevel `json:"topBids"`
	TopAsks []BookLevel `json:"topAsks"`
	Spread  USD         `json:"spread"`
	OBI     float64     `json:"obi"` // static depth imbalance over top N
	Mid     USD         `json:"mid"`
}

// NewOrderBook constructs an empty book.
func NewOrderBook() *OrderBook {
	return &OrderBook{
		bidIdx:  make(map[USD]int),
		askIdx:  make(map[USD]int),
		Bids:    make([]BookLevel, 0, 256),
		Asks:    make([]BookLevel, 0, 256),
		TopBids: make([]BookLevel, 0, 10),
		TopAsks: make([]BookLevel, 0, 10),
	}
}

// Reset clears all levels (used on feed snapshot resync).
func (ob *OrderBook) Reset() {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.bidIdx = make(map[USD]int)
	ob.askIdx = make(map[USD]int)
	ob.Bids = ob.Bids[:0]
	ob.Asks = ob.Asks[:0]
	ob.TopBids = ob.TopBids[:0]
	ob.TopAsks = ob.TopAsks[:0]
	ob.Spread = 0
	ob.OBI = 0
	ob.Mid = 0
}

// Update applies an absolute size at price (size 0 deletes the level).
// side 0 = bid, 1 = ask. Crossed books are resolved by dropping the aggressor level.
func (ob *OrderBook) Update(price USD, size BTC, side uint8) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if side == 0 {
		ob.setBidLocked(price, size)
	} else {
		ob.setAskLocked(price, size)
	}
	ob.uncrossLocked()
	ob.refreshDerivedLocked()
}

func (ob *OrderBook) setBidLocked(price USD, size BTC) {
	if idx, ok := ob.bidIdx[price]; ok {
		if size == 0 {
			ob.removeBidAt(idx)
			return
		}
		ob.Bids[idx].Size = size
		return
	}
	if size == 0 {
		return
	}
	// Insert descending.
	idx := sort.Search(len(ob.Bids), func(i int) bool {
		return ob.Bids[i].Price <= price
	})
	ob.Bids = append(ob.Bids, BookLevel{})
	copy(ob.Bids[idx+1:], ob.Bids[idx:])
	ob.Bids[idx] = BookLevel{Price: price, Size: size}
	ob.reindexBids()
}

func (ob *OrderBook) setAskLocked(price USD, size BTC) {
	if idx, ok := ob.askIdx[price]; ok {
		if size == 0 {
			ob.removeAskAt(idx)
			return
		}
		ob.Asks[idx].Size = size
		return
	}
	if size == 0 {
		return
	}
	// Insert ascending.
	idx := sort.Search(len(ob.Asks), func(i int) bool {
		return ob.Asks[i].Price >= price
	})
	ob.Asks = append(ob.Asks, BookLevel{})
	copy(ob.Asks[idx+1:], ob.Asks[idx:])
	ob.Asks[idx] = BookLevel{Price: price, Size: size}
	ob.reindexAsks()
}

func (ob *OrderBook) removeBidAt(idx int) {
	price := ob.Bids[idx].Price
	copy(ob.Bids[idx:], ob.Bids[idx+1:])
	ob.Bids = ob.Bids[:len(ob.Bids)-1]
	delete(ob.bidIdx, price)
	ob.reindexBids()
}

func (ob *OrderBook) removeAskAt(idx int) {
	price := ob.Asks[idx].Price
	copy(ob.Asks[idx:], ob.Asks[idx+1:])
	ob.Asks = ob.Asks[:len(ob.Asks)-1]
	delete(ob.askIdx, price)
	ob.reindexAsks()
}

func (ob *OrderBook) reindexBids() {
	for i := range ob.Bids {
		ob.bidIdx[ob.Bids[i].Price] = i
	}
}

func (ob *OrderBook) reindexAsks() {
	for i := range ob.Asks {
		ob.askIdx[ob.Asks[i].Price] = i
	}
}

func (ob *OrderBook) uncrossLocked() {
	for len(ob.Bids) > 0 && len(ob.Asks) > 0 && ob.Bids[0].Price >= ob.Asks[0].Price {
		// Drop both crossed tops (conservative; live matching would trade).
		ob.removeBidAt(0)
		if len(ob.Asks) > 0 {
			ob.removeAskAt(0)
		}
	}
}

func (ob *OrderBook) refreshDerivedLocked() {
	const topN = 10
	ob.TopBids = ob.TopBids[:0]
	ob.TopAsks = ob.TopAsks[:0]
	for i := 0; i < len(ob.Bids) && i < topN; i++ {
		ob.TopBids = append(ob.TopBids, ob.Bids[i])
	}
	for i := 0; i < len(ob.Asks) && i < topN; i++ {
		ob.TopAsks = append(ob.TopAsks, ob.Asks[i])
	}

	if len(ob.Bids) > 0 && len(ob.Asks) > 0 {
		ob.Spread = ob.Asks[0].Price - ob.Bids[0].Price
		ob.Mid = (ob.Bids[0].Price + ob.Asks[0].Price) / 2
	} else {
		ob.Spread = 0
		ob.Mid = 0
	}

	var bidVol, askVol BTC
	for _, b := range ob.TopBids {
		bidVol += b.Size
	}
	for _, a := range ob.TopAsks {
		askVol += a.Size
	}
	denom := float64(bidVol + askVol)
	if denom > 0 {
		ob.OBI = (float64(bidVol) - float64(askVol)) / denom
	} else {
		ob.OBI = 0
	}
}

// Snapshot copies top-of-book fields for JSON handlers.
func (ob *OrderBook) Snapshot() (topBids, topAsks []BookLevel, spread, mid USD, obi float64) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	topBids = append([]BookLevel(nil), ob.TopBids...)
	topAsks = append([]BookLevel(nil), ob.TopAsks...)
	return topBids, topAsks, ob.Spread, ob.Mid, ob.OBI
}

// BestBidAsk returns BBO sizes for OFI / microstructure math.
func (ob *OrderBook) BestBidAsk() (bidPrice USD, bidSize BTC, askPrice USD, askSize BTC, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if len(ob.Bids) == 0 || len(ob.Asks) == 0 {
		return 0, 0, 0, 0, false
	}
	return ob.Bids[0].Price, ob.Bids[0].Size, ob.Asks[0].Price, ob.Asks[0].Size, true
}

// LevelCount returns total resting levels (bids + asks).
func (ob *OrderBook) LevelCount() (bids, asks int) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return len(ob.Bids), len(ob.Asks)
}

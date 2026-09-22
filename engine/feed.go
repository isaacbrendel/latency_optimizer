package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
)

// FeedStatus is the live venue-feed lifecycle.
type FeedStatus int32

const (
	FeedStatusDisconnected FeedStatus = iota
	FeedStatusConnecting
	FeedStatusLive
	FeedStatusResyncing
	FeedStatusFallbackMock
)

func (s FeedStatus) String() string {
	switch s {
	case FeedStatusConnecting:
		return "connecting"
	case FeedStatusLive:
		return "live"
	case FeedStatusResyncing:
		return "resyncing"
	case FeedStatusFallbackMock:
		return "fallback_mock"
	default:
		return "disconnected"
	}
}

// L2FeedStats is lock-free telemetry for the venue feed.
type L2FeedStats struct {
	Status        int32
	Messages      int64
	Snapshots     int64
	Updates       int64
	Gaps          int64
	Resyncs       int64
	ParseErrors   int64
	LastSequence  int64
	ConnectedNS   int64
}

// L2Feed applies venue snapshot/delta messages onto the book + ring with
// sequence-gap detection. Transport (WS) is separate; this is the pure engine.
type L2Feed struct {
	ProductID string
	Book      *OrderBook
	Ring      *RingBufferV6
	Micro     *MicrostructureState

	stats L2FeedStats

	lastSeq    int64 // last applied exchange sequence (0 = unknown)
	haveSeq    bool
	needSnap   bool // true after gap until snapshot received
	idCounter  int64
}

// NewL2Feed constructs a feed applicator for productID (e.g. "BTC-USD").
func NewL2Feed(productID string, book *OrderBook, ring *RingBufferV6, micro *MicrostructureState) *L2Feed {
	return &L2Feed{
		ProductID: productID,
		Book:      book,
		Ring:      ring,
		Micro:     micro,
		needSnap:  true,
	}
}

func (f *L2Feed) Stats() L2FeedStats {
	return L2FeedStats{
		Status:       atomic.LoadInt32(&f.stats.Status),
		Messages:     atomic.LoadInt64(&f.stats.Messages),
		Snapshots:    atomic.LoadInt64(&f.stats.Snapshots),
		Updates:      atomic.LoadInt64(&f.stats.Updates),
		Gaps:         atomic.LoadInt64(&f.stats.Gaps),
		Resyncs:      atomic.LoadInt64(&f.stats.Resyncs),
		ParseErrors:  atomic.LoadInt64(&f.stats.ParseErrors),
		LastSequence: atomic.LoadInt64(&f.stats.LastSequence),
		ConnectedNS:  atomic.LoadInt64(&f.stats.ConnectedNS),
	}
}

func (f *L2Feed) SetStatus(s FeedStatus) {
	atomic.StoreInt32(&f.stats.Status, int32(s))
}

// HandleMessage parses one JSON WS frame. Returns true if a full resync is required
// (caller should reconnect / resubscribe).
func (f *L2Feed) HandleMessage(raw []byte) (resync bool, err error) {
	atomic.AddInt64(&f.stats.Messages, 1)

	var envelope struct {
		Type      string `json:"type"`
		ProductID string `json:"product_id"`
		Sequence  int64  `json:"sequence"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		atomic.AddInt64(&f.stats.ParseErrors, 1)
		return false, err
	}

	switch envelope.Type {
	case "snapshot":
		return false, f.applySnapshot(raw, envelope.Sequence)
	case "l2update":
		return f.applyUpdate(raw, envelope.Sequence)
	case "error":
		atomic.AddInt64(&f.stats.ParseErrors, 1)
		return true, fmt.Errorf("venue error frame: %s", string(raw))
	case "subscriptions", "heartbeat":
		return false, nil
	default:
		// ignore ticker etc.
		return false, nil
	}
}

func (f *L2Feed) applySnapshot(raw []byte, seq int64) error {
	var snap struct {
		ProductID string     `json:"product_id"`
		Bids      [][]string `json:"bids"`
		Asks      [][]string `json:"asks"`
		Sequence  int64      `json:"sequence"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		atomic.AddInt64(&f.stats.ParseErrors, 1)
		return err
	}
	if snap.Sequence != 0 {
		seq = snap.Sequence
	}

	f.Book.Reset()
	batch := make([]CompactTrade, 0, len(snap.Bids)+len(snap.Asks))
	now := atomic.AddInt64(&f.idCounter, 1)

	for _, lvl := range snap.Bids {
		px, sz, ok := parseLevel(lvl)
		if !ok {
			continue
		}
		f.Book.Update(px, sz, 0)
		batch = append(batch, CompactTrade{
			ID: now, Price: px, Quantity: sz, Timestamp: now,
			Side: 0, Flags: 1, VenueID: 0, Sequence: uint16(seq),
		})
		now = atomic.AddInt64(&f.idCounter, 1)
	}
	for _, lvl := range snap.Asks {
		px, sz, ok := parseLevel(lvl)
		if !ok {
			continue
		}
		f.Book.Update(px, sz, 1)
		batch = append(batch, CompactTrade{
			ID: now, Price: px, Quantity: sz, Timestamp: now,
			Side: 1, Flags: 1, VenueID: 0, Sequence: uint16(seq),
		})
		now = atomic.AddInt64(&f.idCounter, 1)
	}

	if f.Ring != nil && len(batch) > 0 {
		f.Ring.PublishBatch(batch)
	}
	if f.Micro != nil {
		f.Micro.OnBookUpdate(f.Book)
	}

	if seq > 0 {
		f.lastSeq = seq
		f.haveSeq = true
		atomic.StoreInt64(&f.stats.LastSequence, seq)
	}
	f.needSnap = false
	atomic.AddInt64(&f.stats.Snapshots, 1)
	AddTrace("W", "WRITE", 0, 0, fmt.Sprintf("[L2 snapshot] %s bids=%d asks=%d seq=%d",
		f.ProductID, len(snap.Bids), len(snap.Asks), seq))
	return nil
}

func (f *L2Feed) applyUpdate(raw []byte, seq int64) (resync bool, err error) {
	var upd struct {
		ProductID string     `json:"product_id"`
		Changes   [][]string `json:"changes"`
		Time      string     `json:"time"`
		Sequence  int64      `json:"sequence"`
	}
	if err := json.Unmarshal(raw, &upd); err != nil {
		atomic.AddInt64(&f.stats.ParseErrors, 1)
		return false, err
	}
	if upd.Sequence != 0 {
		seq = upd.Sequence
	}

	if f.needSnap {
		// Drop deltas until we have a snapshot after gap/reconnect.
		return false, nil
	}

	if seq > 0 && f.haveSeq {
		if seq <= f.lastSeq {
			// duplicate / reorder — ignore
			return false, nil
		}
		if seq > f.lastSeq+1 {
			atomic.AddInt64(&f.stats.Gaps, 1)
			atomic.AddInt64(&f.stats.Resyncs, 1)
			f.needSnap = true
			f.haveSeq = false
			AddTrace("W", "WRITE", 0, 0, fmt.Sprintf("[L2 GAP] expected %d got %d — resync", f.lastSeq+1, seq))
			return true, nil
		}
	}

	batch := make([]CompactTrade, 0, len(upd.Changes))
	for _, ch := range upd.Changes {
		if len(ch) < 3 {
			continue
		}
		side := uint8(1)
		if ch[0] == "buy" || ch[0] == "bid" {
			side = 0
		}
		px, err1 := strconv.ParseFloat(ch[1], 64)
		sz, err2 := strconv.ParseFloat(ch[2], 64)
		if err1 != nil || err2 != nil {
			atomic.AddInt64(&f.stats.ParseErrors, 1)
			continue
		}
		price := ToUSD(px)
		qty := ToBTC(sz)
		f.Book.Update(price, qty, side)
		id := atomic.AddInt64(&f.idCounter, 1)
		batch = append(batch, CompactTrade{
			ID: id, Price: price, Quantity: qty, Timestamp: id,
			Side: side, VenueID: 0, Sequence: uint16(seq),
		})
	}

	if f.Ring != nil && len(batch) > 0 {
		f.Ring.PublishBatch(batch)
	}
	if f.Micro != nil {
		f.Micro.OnBookUpdate(f.Book)
	}

	if seq > 0 {
		f.lastSeq = seq
		f.haveSeq = true
		atomic.StoreInt64(&f.stats.LastSequence, seq)
	}
	atomic.AddInt64(&f.stats.Updates, 1)
	return false, nil
}

func parseLevel(lvl []string) (USD, BTC, bool) {
	if len(lvl) < 2 {
		return 0, 0, false
	}
	px, err1 := strconv.ParseFloat(lvl[0], 64)
	sz, err2 := strconv.ParseFloat(lvl[1], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return ToUSD(px), ToBTC(sz), true
}

// SubscribeMessage builds the WS subscribe JSON for level2.
func SubscribeMessage(productIDs ...string) []byte {
	if len(productIDs) == 0 {
		productIDs = []string{"BTC-USD"}
	}
	msg := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": productIDs,
		"channels":    []string{"level2", "heartbeat"},
	}
	b, _ := json.Marshal(msg)
	return b
}

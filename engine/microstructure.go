package engine

import (
	"math"
	"sync"
	"time"
)

// MicrostructureState holds honestly-named indicators derived from the book and tape.
//
//	VWAP = Σ(price×qty) / Σ(qty) over the rolling trade window (fixed-point).
//	OBI  = (bidVol − askVol) / (bidVol + askVol) over top-N depth.
//	OFI  = Cont–Kukanov–Stoikov best-quote order-flow imbalance (cumulative).
//	RSI  = Wilder RSI(14) on mid-price deltas — never a rescaled OBI.
type MicrostructureState struct {
	mu          sync.RWMutex
	VWAP        USD     `json:"vwap"`
	RSI         float64 `json:"rsi"`
	OBI         float64 `json:"obi"`
	OFI         float64 `json:"ofi"`
	TotalVolume BTC     `json:"totalVolume"`
	LastUpdated string  `json:"lastUpdated"`

	sumPQ       int64 // Σ(price×qty) in (USDScale × BTCScale) units
	sumQ        int64 // Σ(qty) in BTCScale units
	ofiAcc      float64
	prevBidPx   USD
	prevBidSz   BTC
	prevAskPx   USD
	prevAskSz   BTC
	havePrevBBO bool

	rsiPeriod  int
	rsiAvgGain float64
	rsiAvgLoss float64
	rsiPrevMid USD
	rsiSeeded  int
	rsiReady   bool
}

// NewMicrostructureState constructs indicator state with Wilder RSI period 14.
func NewMicrostructureState() *MicrostructureState {
	return &MicrostructureState{rsiPeriod: 14}
}

// OnBookUpdate refreshes OBI from the book and accumulates OFI from BBO deltas.
func (m *MicrostructureState) OnBookUpdate(ob *OrderBook) {
	ob.mu.RLock()
	obi := ob.OBI
	var bidPx, askPx USD
	var bidSz, askSz BTC
	ok := len(ob.Bids) > 0 && len(ob.Asks) > 0
	if ok {
		bidPx, bidSz = ob.Bids[0].Price, ob.Bids[0].Size
		askPx, askSz = ob.Asks[0].Price, ob.Asks[0].Size
	}
	mid := ob.Mid
	ob.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.OBI = obi
	m.LastUpdated = time.Now().Format("15:04:05")

	if ok {
		if m.havePrevBBO {
			m.ofiAcc += computeOFI(m.prevBidPx, m.prevBidSz, m.prevAskPx, m.prevAskSz, bidPx, bidSz, askPx, askSz)
			m.OFI = m.ofiAcc
		}
		m.prevBidPx, m.prevBidSz = bidPx, bidSz
		m.prevAskPx, m.prevAskSz = askPx, askSz
		m.havePrevBBO = true
	}
	if mid > 0 {
		m.updateRSILocked(mid)
	}
}

// OnTrade folds a print into rolling VWAP.
func (m *MicrostructureState) OnTrade(price USD, qty BTC) {
	if qty <= 0 || price <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sumPQ += int64(price) * int64(qty)
	m.sumQ += int64(qty)
	m.TotalVolume = BTC(m.sumQ)
	if m.sumQ > 0 {
		m.VWAP = USD(m.sumPQ / m.sumQ)
	}
	m.LastUpdated = time.Now().Format("15:04:05")
}

func (m *MicrostructureState) updateRSILocked(mid USD) {
	if m.rsiPrevMid == 0 {
		m.rsiPrevMid = mid
		return
	}
	change := float64(mid-m.rsiPrevMid) / float64(USDScale)
	m.rsiPrevMid = mid
	gain, loss := 0.0, 0.0
	if change > 0 {
		gain = change
	} else {
		loss = -change
	}
	period := float64(m.rsiPeriod)
	if m.rsiSeeded < m.rsiPeriod {
		m.rsiAvgGain += gain
		m.rsiAvgLoss += loss
		m.rsiSeeded++
		if m.rsiSeeded == m.rsiPeriod {
			m.rsiAvgGain /= period
			m.rsiAvgLoss /= period
			m.rsiReady = true
		}
	} else {
		m.rsiAvgGain = (m.rsiAvgGain*(period-1) + gain) / period
		m.rsiAvgLoss = (m.rsiAvgLoss*(period-1) + loss) / period
		m.rsiReady = true
	}
	if !m.rsiReady {
		m.RSI = 50
		return
	}
	if m.rsiAvgLoss == 0 {
		m.RSI = 100
		return
	}
	rs := m.rsiAvgGain / m.rsiAvgLoss
	m.RSI = 100 - (100 / (1 + rs))
	if math.IsNaN(m.RSI) {
		m.RSI = 50
	}
}

// Snapshot returns a JSON-safe copy of exported fields.
func (m *MicrostructureState) Snapshot() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{
		"vwap":        m.VWAP,
		"rsi":         m.RSI,
		"obi":         m.OBI,
		"ofi":         m.OFI,
		"totalVolume": m.TotalVolume,
		"lastUpdated": m.LastUpdated,
	}
}

// computeOFI implements Cont–Kukanov–Stoikov best-quote OFI for one BBO transition.
func computeOFI(prevBidPx USD, prevBidSz BTC, prevAskPx USD, prevAskSz BTC,
	bidPx USD, bidSz BTC, askPx USD, askSz BTC) float64 {

	var e float64
	switch {
	case bidPx > prevBidPx:
		e += float64(bidSz)
	case bidPx < prevBidPx:
		e -= float64(prevBidSz)
	default:
		e += float64(bidSz - prevBidSz)
	}
	switch {
	case askPx < prevAskPx:
		e -= float64(askSz)
	case askPx > prevAskPx:
		e += float64(prevAskSz)
	default:
		e -= float64(askSz - prevAskSz)
	}
	return e
}

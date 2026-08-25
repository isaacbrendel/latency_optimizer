package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestOrderBook_L2Calculations verifies order book updates, sorting, spread, and OBI calculations.
func TestOrderBook_L2Calculations(t *testing.T) {
	ob := &OrderBook{
		Bids: make(map[USD]BTC),
		Asks: make(map[USD]BTC),
	}

	// Insert bids
	ob.Update(ToUSD(65000.0), ToBTC(1.5), 0) // Bid 1
	ob.Update(ToUSD(64990.0), ToBTC(2.0), 0) // Bid 2
	ob.Update(ToUSD(64980.0), ToBTC(3.0), 0) // Bid 3

	// Insert asks
	ob.Update(ToUSD(65010.0), ToBTC(1.0), 1) // Ask 1
	ob.Update(ToUSD(65020.0), ToBTC(2.5), 1) // Ask 2

	ob.mu.RLock()
	defer ob.mu.RUnlock()

	// Verify top bids sorting (descending)
	if len(ob.TopBids) != 3 {
		t.Fatalf("expected 3 top bids, got %d", len(ob.TopBids))
	}
	if ob.TopBids[0].Price != ToUSD(65000.0) || ob.TopBids[1].Price != ToUSD(64990.0) {
		t.Errorf("Top bids incorrectly sorted: %v", ob.TopBids)
	}

	// Verify top asks sorting (ascending)
	if len(ob.TopAsks) != 2 {
		t.Fatalf("expected 2 top asks, got %d", len(ob.TopAsks))
	}
	if ob.TopAsks[0].Price != ToUSD(65010.0) || ob.TopAsks[1].Price != ToUSD(65020.0) {
		t.Errorf("Top asks incorrectly sorted: %v", ob.TopAsks)
	}

	// Verify spread: 65010 - 65000 = $10.00
	expectedSpread := ToUSD(10.0)
	if ob.Spread != expectedSpread {
		t.Errorf("expected spread %v, got %v", expectedSpread, ob.Spread)
	}

	// Verify OBI calculation
	// Total Bid Vol: 1.5 + 2.0 + 3.0 = 6.5 BTC
	// Total Ask Vol: 1.0 + 2.5 = 3.5 BTC
	// OBI = (6.5 - 3.5) / (6.5 + 3.5) = 3.0 / 10.0 = +0.30
	expectedOBI := 0.30
	if ob.OBI < expectedOBI-0.001 || ob.OBI > expectedOBI+0.001 {
		t.Errorf("expected OBI ~%f, got %f", expectedOBI, ob.OBI)
	}

	// Test level deletion (size 0)
	ob.mu.RUnlock()
	ob.Update(ToUSD(65000.0), 0, 0)
	ob.mu.RLock()

	if len(ob.TopBids) != 2 {
		t.Fatalf("expected 2 top bids after deletion, got %d", len(ob.TopBids))
	}
	if ob.TopBids[0].Price != ToUSD(64990.0) {
		t.Errorf("expected best bid 64990 after deleting 65000, got %v", ob.TopBids[0].Price)
	}
}

// TestOrderBook_CrossingBookResolution verifies that when Best Bid >= Best Ask, the cross is resolved.
func TestOrderBook_CrossingBookResolution(t *testing.T) {
	ob := &OrderBook{
		Bids: make(map[USD]BTC),
		Asks: make(map[USD]BTC),
	}

	ob.Update(ToUSD(65000.0), ToBTC(1.0), 0) // Bid at 65000
	ob.Update(ToUSD(65010.0), ToBTC(1.0), 1) // Ask at 65010

	// Crossed order: new Bid at 65020 (>= Best Ask 65010)
	ob.Update(ToUSD(65020.0), ToBTC(1.0), 0)

	ob.mu.RLock()
	defer ob.mu.RUnlock()

	// Best bid and best ask that crossed should be deleted/matched
	if len(ob.TopBids) > 0 && len(ob.TopAsks) > 0 {
		if ob.TopBids[0].Price >= ob.TopAsks[0].Price {
			t.Errorf("crossed book was not resolved: Best Bid %v >= Best Ask %v",
				ob.TopBids[0].Price, ob.TopAsks[0].Price)
		}
	}
}

// TestEventTrace_CircularCap verifies that event traces keep at most the last 20 events.
func TestEventTrace_CircularCap(t *testing.T) {
	for i := 1; i <= 30; i++ {
		AddTrace("W", "WRITE", int64(i%32), int64(i), "Test Event")
	}

	recent := GetTraces()
	if len(recent) > 20 {
		t.Fatalf("expected at most 20 traces, got %d", len(recent))
	}
	// Last trace should have tradeID 30
	last := recent[len(recent)-1]
	if last.TradeID != 30 {
		t.Errorf("expected last trace trade ID 30, got %d", last.TradeID)
	}
}

// TestHTTP_OrderBookAPI verifies the /api/orderbook JSON endpoint.
func TestHTTP_OrderBookAPI(t *testing.T) {
	EnsureInitialized()

	req := httptest.NewRequest(http.MethodGet, "/api/orderbook", nil)
	rec := httptest.NewRecorder()

	HandleOrderBookAPI(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected application/json, got %s", contentType)
	}

	var payload struct {
		OrderBook OrderBook `json:"orderBook"`
		MidPrice  USD       `json:"midPrice"`
		Trades    []*Trade  `json:"trades"`
		Bot       BotState  `json:"bot"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode /api/orderbook JSON: %v", err)
	}

	if payload.Bot.Cash == 0 && payload.Bot.NAV == 0 {
		t.Errorf("bot state in response is empty: %+v", payload.Bot)
	}
}

// TestHTTP_RingBufferAPI verifies the /api/ring-buffer JSON endpoint.
func TestHTTP_RingBufferAPI(t *testing.T) {
	EnsureInitialized()

	req := httptest.NewRequest(http.MethodGet, "/api/ring-buffer", nil)
	rec := httptest.NewRecorder()

	HandleRingBufferAPI(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	var payload struct {
		WriteSeq     int64        `json:"writeSeq"`
		BotSeq       int64        `json:"botSeq"`
		AISeq        int64        `json:"aiSeq"`
		AuditSeq     int64        `json:"auditSeq"`
		EvictedCount int64        `json:"evictedCount"`
		Slots        []struct {
			Index   int64   `json:"index"`
			TradeID int64   `json:"tradeId"`
			Price   float64 `json:"price"`
			Side    string  `json:"side"`
			Venue   string  `json:"venue"`
		} `json:"slots"`
		Traces []EventTrace `json:"traces"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode /api/ring-buffer JSON: %v", err)
	}

	if len(payload.Slots) != 32 {
		t.Errorf("expected 32 circular inspection slots, got %d", len(payload.Slots))
	}
}

// TestHTTP_SentimentAPI verifies the /api/sentiment JSON endpoint.
func TestHTTP_SentimentAPI(t *testing.T) {
	EnsureInitialized()

	req := httptest.NewRequest(http.MethodGet, "/api/sentiment", nil)
	rec := httptest.NewRecorder()

	HandleSentimentAPI(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	var sentiment IndicatorState
	if err := json.Unmarshal(rec.Body.Bytes(), &sentiment); err != nil {
		t.Fatalf("failed to decode /api/sentiment JSON: %v", err)
	}
}

// TestHTTP_RunExperimentAPI verifies the SSE benchmark execution streaming endpoint.
func TestHTTP_RunExperimentAPI(t *testing.T) {
	EnsureInitialized()

	req := httptest.NewRequest(http.MethodGet, "/api/run-experiment?trades=100&subscribers=10", nil)
	rec := httptest.NewRecorder()

	HandleRunExperimentAPI(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/event-stream" {
		t.Errorf("expected text/event-stream, got %s", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "[DONE]") {
		t.Errorf("expected SSE stream to conclude with [DONE], got: %s", body)
	}
}

// TestRingBuffer_EngineWaitStrategies verifies wait strategies in the engine package.
func TestRingBuffer_EngineWaitStrategies(t *testing.T) {
	rb := NewRingBufferV6(64, 1)
	rb.SetWaitStrategy("Adaptive")

	trades := []CompactTrade{
		{ID: 1, Price: ToUSD(100.0)},
		{ID: 2, Price: ToUSD(200.0)},
	}

	rb.PublishBatch(trades)

	var readCount int64
	rb.Read(rb.Readers[0], 2, nil, func(ct CompactTrade) {
		atomic.AddInt64(&readCount, 1)
	})

	if atomic.LoadInt64(&readCount) != 2 {
		t.Errorf("expected 2 trades read, got %d", atomic.LoadInt64(&readCount))
	}
}

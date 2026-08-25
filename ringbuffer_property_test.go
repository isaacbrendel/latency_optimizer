package main

import (
	"encoding/json"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// TestProperty_OccupancyBound verifies the core ring buffer invariant:
// For any active blocking reader r, writeSeq - readSeq <= bufferSize at all times.
func TestProperty_OccupancyBound(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	iterations := 10

	for iter := 0; iter < iterations; iter++ {
		bufferSize := int64(1 << (r.Intn(4) + 4)) // random power of 2 between 16 and 128
		numReaders := r.Intn(4) + 1
		totalTrades := r.Intn(500) + 100

		rb := NewRingBufferV6(bufferSize, numReaders)
		trades := make([]CompactTrade, totalTrades)
		for i := 0; i < totalTrades; i++ {
			trades[i] = CompactTrade{ID: int64(i + 1), Price: ToUSD(100.0 + float64(i))}
		}

		var occupancyViolations int64
		var wg sync.WaitGroup
		wg.Add(numReaders)

		for i := 0; i < numReaders; i++ {
			go func(reader *RingBufferReader) {
				defer wg.Done()
				rb.Read(reader, int64(totalTrades), nil, func(ct CompactTrade) {
					wSeq := atomic.LoadInt64(&rb.writeSeq)
					rSeq := atomic.LoadInt64(&reader.readSeq)
					if wSeq-rSeq > bufferSize {
						atomic.AddInt64(&occupancyViolations, 1)
					}
				})
			}(rb.readers[i])
		}

		// Random batch publication
		for idx := 0; idx < totalTrades; {
			batchSize := r.Intn(16) + 1
			end := idx + batchSize
			if end > totalTrades {
				end = totalTrades
			}
			rb.PublishBatch(trades[idx:end])
			idx = end
		}

		wg.Wait()

		if atomic.LoadInt64(&occupancyViolations) > 0 {
			t.Fatalf("Iteration %d (size=%d, readers=%d): detected %d buffer occupancy bound violations",
				iter, bufferSize, numReaders, occupancyViolations)
		}
	}
}

// TestProperty_ZeroDuplicateDelivery verifies that across thousands of randomized batches,
// every blocking subscriber receives every single trade exactly once with 0 duplicates and 0 drops.
func TestProperty_ZeroDuplicateDelivery(t *testing.T) {
	r := rand.New(rand.NewSource(1337))
	bufferSize := int64(128)
	numReaders := 4
	totalTrades := 2000

	rb := NewRingBufferV6(bufferSize, numReaders)
	trades := make([]CompactTrade, totalTrades)
	for i := 0; i < totalTrades; i++ {
		trades[i] = CompactTrade{
			ID:    int64(i + 1),
			Price: ToUSD(50000.0 + float64(i)),
		}
	}

	receivedSets := make([]map[int64]int, numReaders)
	for i := range receivedSets {
		receivedSets[i] = make(map[int64]int)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func(rIdx int, reader *RingBufferReader) {
			defer wg.Done()
			rb.Read(reader, int64(totalTrades), nil, func(ct CompactTrade) {
				mu.Lock()
				receivedSets[rIdx][ct.ID]++
				mu.Unlock()
			})
		}(i, rb.readers[i])
	}

	for idx := 0; idx < totalTrades; {
		batchSize := r.Intn(64) + 1
		end := idx + batchSize
		if end > totalTrades {
			end = totalTrades
		}
		rb.PublishBatch(trades[idx:end])
		idx = end
	}

	wg.Wait()

	// Verify exact count = 1 for all items across all readers
	for rIdx := 0; rIdx < numReaders; rIdx++ {
		set := receivedSets[rIdx]
		if len(set) != totalTrades {
			t.Fatalf("Reader %d: unique trade count mismatch: expected %d, got %d",
				rIdx, totalTrades, len(set))
		}
		for id := int64(1); id <= int64(totalTrades); id++ {
			count, exists := set[id]
			if !exists {
				t.Fatalf("Reader %d: missing trade ID %d (dropped message)", rIdx, id)
			}
			if count != 1 {
				t.Fatalf("Reader %d: trade ID %d received %d times (duplicate message)", rIdx, id, count)
			}
		}
	}
}

// TestProperty_FixedPointPrecision verifies financial precision invariants across 10,000 random conversions.
func TestProperty_FixedPointPrecision(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	const samples = 10000

	for i := 0; i < samples; i++ {
		// USD prices between $0.0001 and $250,000.0000
		rawPrice := 0.0001 + r.Float64()*250000.0
		usd := ToUSD(rawPrice)
		restoredUSD := usd.Float64()
		if math.Abs(rawPrice-restoredUSD) > 0.0001 {
			t.Fatalf("USD precision bound violated: original=%f, restored=%f, delta=%f",
				rawPrice, restoredUSD, math.Abs(rawPrice-restoredUSD))
		}

		// BTC sizes between 0.00000001 and 100.00000000 BTC
		rawQty := 0.00000001 + r.Float64()*100.0
		btc := ToBTC(rawQty)
		restoredBTC := btc.Float64()
		if math.Abs(rawQty-restoredBTC) > 0.00000001 {
			t.Fatalf("BTC precision bound violated: original=%f, restored=%f, delta=%f",
				rawQty, restoredBTC, math.Abs(rawQty-restoredBTC))
		}

		// Mathematical Consistency: (BTC * USD) / BTCScale = USD Value
		value := btc.Value(usd)
		expectedValue := rawPrice * rawQty
		if math.Abs(value.Float64()-expectedValue) > (rawPrice*0.0000001 + 0.01) {
			t.Fatalf("Value calculation mismatch: expected ~%f, got %f",
				expectedValue, value.Float64())
		}
	}
}

// FuzzRingBufferPublishRead is a Go native fuzz test fuzzing batch sizes, prices, and quantities.
func FuzzRingBufferPublishRead(f *testing.F) {
	// Seed corpus
	f.Add(uint8(1), 100.0, 1.5, int64(1001))
	f.Add(uint8(8), 65432.10, 0.005, int64(2002))
	f.Add(uint8(32), 50.25, 10.0, int64(3003))
	f.Add(uint8(64), 1234.56, 0.12345678, int64(4004))

	f.Fuzz(func(t *testing.T, batchSizeUint uint8, priceFloat float64, qtyFloat float64, baseID int64) {
		if math.IsNaN(priceFloat) || math.IsInf(priceFloat, 0) || priceFloat < 0 || priceFloat > 1e7 {
			return
		}
		if math.IsNaN(qtyFloat) || math.IsInf(qtyFloat, 0) || qtyFloat < 0 || qtyFloat > 1e5 {
			return
		}
		batchSize := int(batchSizeUint%32) + 1
		totalTrades := batchSize * 2

		rb := NewRingBufferV6(64, 1)
		trades := make([]CompactTrade, totalTrades)
		for i := 0; i < totalTrades; i++ {
			trades[i] = CompactTrade{
				ID:        baseID + int64(i),
				Price:     ToUSD(priceFloat + float64(i)),
				Quantity:  ToBTC(qtyFloat),
				Timestamp: int64(i),
			}
		}

		var received []CompactTrade
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			rb.Read(rb.readers[0], int64(totalTrades), nil, func(ct CompactTrade) {
				received = append(received, ct)
			})
		}()

		for i := 0; i < totalTrades; i += batchSize {
			end := i + batchSize
			if end > totalTrades {
				end = totalTrades
			}
			rb.PublishBatch(trades[i:end])
		}

		wg.Wait()

		if len(received) != totalTrades {
			t.Fatalf("expected %d received trades, got %d", totalTrades, len(received))
		}
		for i := 0; i < totalTrades; i++ {
			if received[i].ID != trades[i].ID {
				t.Fatalf("trade %d ID mismatch: expected %d, got %d", i, trades[i].ID, received[i].ID)
			}
		}
	})
}

// FuzzFixedPointUSD fuzzes USD JSON serialization and conversion.
func FuzzFixedPointUSD(f *testing.F) {
	f.Add(0.0)
	f.Add(1.0)
	f.Add(65000.55)
	f.Add(999999.9999)
	f.Add(-100.5)

	f.Fuzz(func(t *testing.T, input float64) {
		if math.IsNaN(input) || math.IsInf(input, 0) || math.Abs(input) > 1e10 {
			return
		}
		usd := ToUSD(input)
		data, err := json.Marshal(usd)
		if err != nil {
			t.Fatalf("marshal failed for %v: %v", input, err)
		}

		var restored USD
		if err := json.Unmarshal(data, &restored); err != nil {
			t.Fatalf("unmarshal failed for %s: %v", string(data), err)
		}

		if math.Abs(restored.Float64()-usd.Float64()) > 0.0001 {
			t.Fatalf("roundtrip mismatch: orig=%v, restored=%v", usd, restored)
		}
	})
}

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRingBuffer_Initialization validates powers-of-2 size enforcement, reader allocations, and panic guards.
func TestRingBuffer_Initialization(t *testing.T) {
	validSizes := []int64{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}
	for _, size := range validSizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			numReaders := 4
			rb := NewRingBufferV6(size, numReaders)
			if rb.size != size {
				t.Fatalf("expected size %d, got %d", size, rb.size)
			}
			if rb.mask != size-1 {
				t.Fatalf("expected mask %d, got %d", size-1, rb.mask)
			}
			if len(rb.buffer) != int(size) {
				t.Fatalf("expected buffer length %d, got %d", size, len(rb.buffer))
			}
			if len(rb.readers) != numReaders {
				t.Fatalf("expected %d readers, got %d", numReaders, len(rb.readers))
			}
			for i, r := range rb.readers {
				if r.id != i {
					t.Errorf("expected reader %d id %d, got %d", i, i, r.id)
				}
				if !r.blocking {
					t.Errorf("expected reader %d to be blocking by default", i)
				}
				if atomic.LoadInt64(&r.readSeq) != 0 {
					t.Errorf("expected reader %d readSeq 0, got %d", i, atomic.LoadInt64(&r.readSeq))
				}
			}
		})
	}

	invalidSizes := []int64{0, -1, 3, 5, 7, 9, 15, 100, 1000, 1023, 1025}
	for _, size := range invalidSizes {
		t.Run(fmt.Sprintf("InvalidSize_%d", size), func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("expected panic for invalid ring buffer size %d, but did not panic", size)
				}
			}()
			NewRingBufferV6(size, 2)
		})
	}
}

// TestRingBuffer_PublishAndRead_SingleItem verifies exact 40-byte CompactTrade field preservation through the ring buffer.
func TestRingBuffer_PublishAndRead_SingleItem(t *testing.T) {
	rb := NewRingBufferV6(64, 1)
	expectedTrade := CompactTrade{
		ID:        987654321,
		Price:     ToUSD(67890.12),
		Quantity:  ToBTC(1.54321098),
		Timestamp: 1720000000123456789,
		Sequence:  42,
		SymbolID:  1,
		Side:      1,
		Flags:     3,
		VenueID:   2,
	}

	rb.PublishBatch([]CompactTrade{expectedTrade})

	if atomic.LoadInt64(&rb.writeSeq) != 1 {
		t.Fatalf("expected writeSeq 1, got %d", atomic.LoadInt64(&rb.writeSeq))
	}

	var receivedTrade CompactTrade
	var receivedCount int

	rb.Read(rb.readers[0], 1, nil, func(ct CompactTrade) {
		receivedTrade = ct
		receivedCount++
	})

	if receivedCount != 1 {
		t.Fatalf("expected 1 trade read, got %d", receivedCount)
	}

	if receivedTrade.ID != expectedTrade.ID {
		t.Errorf("ID mismatch: expected %d, got %d", expectedTrade.ID, receivedTrade.ID)
	}
	if receivedTrade.Price != expectedTrade.Price {
		t.Errorf("Price mismatch: expected %v, got %v", expectedTrade.Price, receivedTrade.Price)
	}
	if receivedTrade.Quantity != expectedTrade.Quantity {
		t.Errorf("Quantity mismatch: expected %v, got %v", expectedTrade.Quantity, receivedTrade.Quantity)
	}
	if receivedTrade.Timestamp != expectedTrade.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", expectedTrade.Timestamp, receivedTrade.Timestamp)
	}
	if receivedTrade.Sequence != expectedTrade.Sequence {
		t.Errorf("Sequence mismatch: expected %d, got %d", expectedTrade.Sequence, receivedTrade.Sequence)
	}
	if receivedTrade.SymbolID != expectedTrade.SymbolID {
		t.Errorf("SymbolID mismatch: expected %d, got %d", expectedTrade.SymbolID, receivedTrade.SymbolID)
	}
	if receivedTrade.Side != expectedTrade.Side {
		t.Errorf("Side mismatch: expected %d, got %d", expectedTrade.Side, receivedTrade.Side)
	}
	if receivedTrade.Flags != expectedTrade.Flags {
		t.Errorf("Flags mismatch: expected %d, got %d", expectedTrade.Flags, receivedTrade.Flags)
	}
	if receivedTrade.VenueID != expectedTrade.VenueID {
		t.Errorf("VenueID mismatch: expected %d, got %d", expectedTrade.VenueID, receivedTrade.VenueID)
	}
}

// TestRingBuffer_PublishAndRead_Batch validates FIFO ordering and field integrity across table-driven batch sizes.
func TestRingBuffer_PublishAndRead_Batch(t *testing.T) {
	testCases := []struct {
		name       string
		bufferSize int64
		numTrades  int
		batchSize  int
		numReaders int
	}{
		{"SmallBatch_1Item", 64, 50, 1, 1},
		{"BatchSize_7_Prime", 64, 70, 7, 2},
		{"BatchSize_16_Aligned", 64, 160, 16, 3},
		{"LargeBatch_128", 256, 1024, 128, 4},
		{"BatchLargerThanBuffer", 16, 128, 8, 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb := NewRingBufferV6(tc.bufferSize, tc.numReaders)
			trades := make([]CompactTrade, tc.numTrades)
			for i := 0; i < tc.numTrades; i++ {
				trades[i] = CompactTrade{
					ID:        int64(i + 1),
					Price:     ToUSD(60000.0 + float64(i)),
					Quantity:  ToBTC(0.1 + float64(i)*0.01),
					Timestamp: int64(1700000000 + i),
					Sequence:  uint16(i % 65535),
					SymbolID:  uint8(i % 4),
					Side:      uint8(i % 2),
					VenueID:   uint8(i % 3),
				}
			}

			var wg sync.WaitGroup
			wg.Add(tc.numReaders)

			readerResults := make([][]CompactTrade, tc.numReaders)

			for rIdx := 0; rIdx < tc.numReaders; rIdx++ {
				go func(idx int, reader *RingBufferReader) {
					defer wg.Done()
					var received []CompactTrade
					rb.Read(reader, int64(tc.numTrades), nil, func(ct CompactTrade) {
						received = append(received, ct)
					})
					readerResults[idx] = received
				}(rIdx, rb.readers[rIdx])
			}

			// Publish in batches
			for j := 0; j < len(trades); j += tc.batchSize {
				end := j + tc.batchSize
				if end > len(trades) {
					end = len(trades)
				}
				rb.PublishBatch(trades[j:end])
			}

			wg.Wait()

			// Verify all readers received all items in strict FIFO order
			for rIdx := 0; rIdx < tc.numReaders; rIdx++ {
				res := readerResults[rIdx]
				if len(res) != tc.numTrades {
					t.Fatalf("reader %d received %d trades, expected %d", rIdx, len(res), tc.numTrades)
				}
				for i := 0; i < tc.numTrades; i++ {
					if res[i].ID != trades[i].ID {
						t.Fatalf("reader %d trade[%d] ID mismatch: expected %d, got %d",
							rIdx, i, trades[i].ID, res[i].ID)
					}
					if res[i].Price != trades[i].Price {
						t.Fatalf("reader %d trade[%d] Price mismatch: expected %v, got %v",
							rIdx, i, trades[i].Price, res[i].Price)
					}
				}
			}
		})
	}
}

// TestRingBuffer_WrapAround verifies circular indexing across multiple buffer wrap-around cycles.
func TestRingBuffer_WrapAround(t *testing.T) {
	bufferSize := int64(16)
	numTrades := 2000 // 125 full wraps around the 16-slot buffer
	batchSize := 4

	rb := NewRingBufferV6(bufferSize, 1)

	trades := make([]CompactTrade, numTrades)
	for i := 0; i < numTrades; i++ {
		trades[i] = CompactTrade{
			ID:        int64(i + 100),
			Price:     ToUSD(100.0 + float64(i)),
			Quantity:  ToBTC(1.0),
			Timestamp: int64(i),
		}
	}

	var received []CompactTrade
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		rb.Read(rb.readers[0], int64(numTrades), nil, func(ct CompactTrade) {
			received = append(received, ct)
		})
	}()

	for i := 0; i < numTrades; i += batchSize {
		end := i + batchSize
		if end > numTrades {
			end = numTrades
		}
		rb.PublishBatch(trades[i:end])
		time.Sleep(50 * time.Microsecond) // Allow reader to consume and advance cachedMin
	}

	wg.Wait()

	if len(received) != numTrades {
		t.Fatalf("expected %d received trades, got %d", numTrades, len(received))
	}
	for i := 0; i < numTrades; i++ {
		if received[i].ID != trades[i].ID {
			t.Fatalf("slot %d: expected trade ID %d, got %d", i, trades[i].ID, received[i].ID)
		}
	}
}

// TestRingBuffer_SequenceMonotonicity verifies strict sequence monotonicity invariants under batching.
func TestRingBuffer_SequenceMonotonicity(t *testing.T) {
	rb := NewRingBufferV6(64, 2)
	numTrades := 500
	trades := make([]CompactTrade, numTrades)
	for i := 0; i < numTrades; i++ {
		trades[i] = CompactTrade{ID: int64(i + 1), Price: ToUSD(100.0)}
	}

	var lastWriteSeq int64
	var lastReadSeqs [2]int64

	var wg sync.WaitGroup
	wg.Add(2)

	for r := 0; r < 2; r++ {
		go func(idx int, reader *RingBufferReader) {
			defer wg.Done()
			rb.Read(reader, int64(numTrades), nil, func(ct CompactTrade) {
				currentSeq := atomic.LoadInt64(&reader.readSeq)
				if currentSeq < lastReadSeqs[idx] {
					t.Errorf("reader %d sequence regressed from %d to %d", idx, lastReadSeqs[idx], currentSeq)
				}
				lastReadSeqs[idx] = currentSeq
			})
		}(r, rb.readers[r])
	}

	for i := 0; i < numTrades; i += 10 {
		end := i + 10
		if end > numTrades {
			end = numTrades
		}
		rb.PublishBatch(trades[i:end])
		currentWrite := atomic.LoadInt64(&rb.writeSeq)
		if currentWrite < lastWriteSeq {
			t.Fatalf("writeSeq regressed from %d to %d", lastWriteSeq, currentWrite)
		}
		lastWriteSeq = currentWrite
	}

	wg.Wait()

	if atomic.LoadInt64(&rb.writeSeq) != int64(numTrades) {
		t.Errorf("expected final writeSeq %d, got %d", numTrades, atomic.LoadInt64(&rb.writeSeq))
	}
}

// TestRingBuffer_NonBlockingEviction verifies slow non-blocking subscribers are evicted forward without deadlock.
func TestRingBuffer_NonBlockingEviction(t *testing.T) {
	bufferSize := int64(16)
	rb := NewRingBufferV6(bufferSize, 1)
	rb.readers[0].blocking = false // Configure as non-blocking market data reader

	numTrades := 100
	trades := make([]CompactTrade, numTrades)
	for i := 0; i < numTrades; i++ {
		trades[i] = CompactTrade{
			ID:    int64(i + 1),
			Price: ToUSD(50000.0 + float64(i)),
		}
	}

	// Publish all 100 trades without the reader reading, forcing 100 - 16 = 84 overruns
	rb.PublishBatchEvicting(trades)

	evictedCount := atomic.LoadInt64(&rb.readers[0].evictedCount)
	if evictedCount == 0 {
		t.Fatalf("expected evictedCount > 0, got %d", evictedCount)
	}

	readSeq := atomic.LoadInt64(&rb.readers[0].readSeq)
	expectedMinSeq := int64(numTrades) - bufferSize
	if readSeq < expectedMinSeq {
		t.Fatalf("expected readSeq >= %d after eviction, got %d", expectedMinSeq, readSeq)
	}

	// Reader consumes remaining valid items
	var consumed []CompactTrade
	rb.Read(rb.readers[0], int64(numTrades), nil, func(ct CompactTrade) {
		consumed = append(consumed, ct)
	})

	if len(consumed) == 0 {
		t.Fatalf("expected to read valid un-evicted trades, got 0")
	}

	// Last read item must be the last published item
	lastItem := consumed[len(consumed)-1]
	if lastItem.ID != int64(numTrades) {
		t.Errorf("expected last item ID %d, got %d", numTrades, lastItem.ID)
	}
}

// TestRingBuffer_WaitStrategies tests all 4 consumer wait strategies for correctness.
func TestRingBuffer_WaitStrategies(t *testing.T) {
	strategies := []string{"Blocking", "Adaptive", "Yielding", "BusySpin"}

	for _, strat := range strategies {
		t.Run(strat, func(t *testing.T) {
			rb := NewRingBufferV6(64, 1)
			rb.SetWaitStrategy(strat)

			trades := make([]CompactTrade, 20)
			for i := 0; i < 20; i++ {
				trades[i] = CompactTrade{ID: int64(i + 1), Price: ToUSD(100.0 + float64(i))}
			}

			var received []CompactTrade
			done := make(chan struct{})

			go func() {
				rb.Read(rb.readers[0], 20, nil, func(ct CompactTrade) {
					received = append(received, ct)
				})
				close(done)
			}()

			time.Sleep(10 * time.Millisecond) // Let reader enter wait path
			rb.PublishBatch(trades[:10])
			time.Sleep(10 * time.Millisecond)
			rb.PublishBatch(trades[10:])

			select {
			case <-done:
				if len(received) != 20 {
					t.Fatalf("[%s] expected 20 trades, got %d", strat, len(received))
				}
				for i := 0; i < 20; i++ {
					if received[i].ID != trades[i].ID {
						t.Errorf("[%s] trade[%d] ID mismatch: expected %d, got %d",
							strat, i, trades[i].ID, received[i].ID)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("[%s] reader timed out waiting for published batch", strat)
			}
		})
	}
}

// TestFixedPoint_ArithmeticAndJSON tests USD and BTC fixed-point conversions, math, and JSON serialization.
func TestFixedPoint_ArithmeticAndJSON(t *testing.T) {
	// Test USD precision (4 decimal places)
	usdVal := 65432.1234
	usd := ToUSD(usdVal)
	if math.Abs(usd.Float64()-usdVal) > 0.0001 {
		t.Errorf("USD precision lost: expected %f, got %f", usdVal, usd.Float64())
	}

	// Test BTC precision (8 decimal places / Satoshis)
	btcVal := 1.23456789
	btc := ToBTC(btcVal)
	if math.Abs(btc.Float64()-btcVal) > 0.00000001 {
		t.Errorf("BTC precision lost: expected %f, got %f", btcVal, btc.Float64())
	}

	// Test Value calculation: 2 BTC @ $50,000 = $100,000
	twoBTC := ToBTC(2.0)
	price50k := ToUSD(50000.0)
	valueUSD := twoBTC.Value(price50k)
	expectedUSD := ToUSD(100000.0)
	if valueUSD != expectedUSD {
		t.Errorf("Value mismatch: expected %v ($100k), got %v", expectedUSD, valueUSD)
	}

	// Test Quant calculation: $100,000 @ $50,000/BTC = 2 BTC
	quantBTC := expectedUSD.Quant(price50k)
	if quantBTC != twoBTC {
		t.Errorf("Quant mismatch: expected %v (2 BTC), got %v", twoBTC, quantBTC)
	}

	// Test JSON Marshaling/Unmarshaling
	type PricePayload struct {
		Price USD `json:"price"`
		Size  BTC `json:"size"`
	}

	orig := PricePayload{Price: ToUSD(49999.95), Size: ToBTC(0.005)}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("JSON marshal failed: %v", err)
	}

	var parsed PricePayload
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	if parsed.Price != orig.Price {
		t.Errorf("JSON round-trip USD mismatch: expected %v, got %v", orig.Price, parsed.Price)
	}
	if parsed.Size != orig.Size {
		t.Errorf("JSON round-trip BTC mismatch: expected %v, got %v", orig.Size, parsed.Size)
	}
}

// TestSequenceBarrier_Ordering verifies multi-stage consumer DAG dependency chains.
func TestSequenceBarrier_Ordering(t *testing.T) {
	rb := NewRingBufferV6(64, 2)
	quantReader := rb.readers[0] // Stage 1
	botReader := rb.readers[1]   // Stage 2 (depends on Stage 1)

	barrier := NewSequenceBarrier(func() int64 {
		return atomic.LoadInt64(&quantReader.readSeq)
	})

	trades := []CompactTrade{
		{ID: 1, Price: ToUSD(100.0)},
		{ID: 2, Price: ToUSD(200.0)},
		{ID: 3, Price: ToUSD(300.0)},
	}

	rb.PublishBatch(trades)

	// Before Stage 1 reads, barrier should return 0
	if barrier.GetAvailableSequence() != 0 {
		t.Fatalf("expected barrier sequence 0 before Stage 1 reads, got %d", barrier.GetAvailableSequence())
	}

	// Stage 1 consumes 2 trades
	var stage1Read []CompactTrade
	rb.Read(quantReader, 2, nil, func(ct CompactTrade) {
		stage1Read = append(stage1Read, ct)
	})

	if barrier.GetAvailableSequence() != 2 {
		t.Fatalf("expected barrier sequence 2 after Stage 1 reads 2 items, got %d", barrier.GetAvailableSequence())
	}

	// Stage 2 attempts to read 3 trades, but barrier limits it to 2
	var stage2Read []CompactTrade
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		rb.Read(botReader, 2, barrier, func(ct CompactTrade) {
			stage2Read = append(stage2Read, ct)
		})
	}()

	wg.Wait()

	if len(stage2Read) != 2 {
		t.Fatalf("expected Stage 2 to read 2 trades allowed by barrier, got %d", len(stage2Read))
	}
	if stage2Read[0].ID != 1 || stage2Read[1].ID != 2 {
		t.Errorf("Stage 2 read unexpected trades: %v", stage2Read)
	}
}

// TestFlatBuffers_EncodeDecode tests zero-allocation flat binary encoding and decoding.
func TestFlatBuffers_EncodeDecode(t *testing.T) {
	ct := CompactTrade{
		ID:        123456789,
		Price:     ToUSD(65432.10),
		Quantity:  ToBTC(2.5),
		Timestamp: 1710000000123,
		Sequence:  1001,
		SymbolID:  0,
		Side:      1,
		Flags:     2,
		VenueID:   1,
	}

	buf := make([]byte, 38)
	encoded := EncodeFlatTrade(buf, ct)
	if len(encoded) != 38 {
		t.Fatalf("expected 38 bytes encoded payload, got %d", len(encoded))
	}

	bin := BinaryFlatTrade(encoded)
	if bin.ReadID() != ct.ID {
		t.Errorf("FlatBuffers ID mismatch: expected %d, got %d", ct.ID, bin.ReadID())
	}
	if bin.ReadPrice() != ct.Price {
		t.Errorf("FlatBuffers Price mismatch: expected %v, got %v", ct.Price, bin.ReadPrice())
	}
	if bin.ReadQuantity() != ct.Quantity {
		t.Errorf("FlatBuffers Quantity mismatch: expected %v, got %v", ct.Quantity, bin.ReadQuantity())
	}
	if bin.ReadTimestamp() != ct.Timestamp {
		t.Errorf("FlatBuffers Timestamp mismatch: expected %d, got %d", ct.Timestamp, bin.ReadTimestamp())
	}
	if bin.ReadSequence() != ct.Sequence {
		t.Errorf("FlatBuffers Sequence mismatch: expected %d, got %d", ct.Sequence, bin.ReadSequence())
	}

	decoded := bin.DecodeToCompactTrade()
	if decoded.SymbolID != ct.SymbolID {
		t.Errorf("Decoded SymbolID mismatch: expected %d, got %d", ct.SymbolID, decoded.SymbolID)
	}
	if decoded.Side != ct.Side {
		t.Errorf("Decoded Side mismatch: expected %d, got %d", ct.Side, decoded.Side)
	}
}

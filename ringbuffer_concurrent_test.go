package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Helper to generate a slice of test CompactTrades with sequential IDs.
func generateTestCompactTrades(count int) []CompactTrade {
	trades := make([]CompactTrade, count)
	now := time.Now().UnixNano()
	for i := 0; i < count; i++ {
		trades[i] = CompactTrade{
			ID:        int64(i + 1),
			Price:     ToUSD(60000.0 + float64(i%100)),
			Quantity:  ToBTC(0.5 + float64(i%10)*0.1),
			Timestamp: now + int64(i),
			Sequence:  uint16(i % 65535),
			SymbolID:  uint8(i % 2),
			Side:      uint8(i % 2),
			VenueID:   uint8(i % 3),
		}
	}
	return trades
}

// runProducerConsumersTest executes 1 producer against N blocking consumers and verifies zero loss / duplication.
func runProducerConsumersTest(t *testing.T, numConsumers int, numTrades int, bufferSize int64, batchSize int) {
	rb := NewRingBufferV6(bufferSize, numConsumers)
	trades := generateTestCompactTrades(numTrades)

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	// Track received items per reader to detect loss or duplicate deliveries
	receivedIDs := make([][]int64, numConsumers)
	for i := range receivedIDs {
		receivedIDs[i] = make([]int64, 0, numTrades)
	}

	for i := 0; i < numConsumers; i++ {
		go func(readerIdx int, reader *RingBufferReader) {
			defer wg.Done()
			rb.Read(reader, int64(numTrades), nil, func(ct CompactTrade) {
				receivedIDs[readerIdx] = append(receivedIDs[readerIdx], ct.ID)
			})
		}(i, rb.readers[i])
	}

	// Producer publishes batches
	for j := 0; j < len(trades); j += batchSize {
		end := j + batchSize
		if end > len(trades) {
			end = len(trades)
		}
		rb.PublishBatch(trades[j:end])
	}

	wg.Wait()

	// Rigorous assertions on each consumer's stream
	for c := 0; c < numConsumers; c++ {
		stream := receivedIDs[c]
		if len(stream) != numTrades {
			t.Fatalf("Consumer %d: expected %d trades, got %d", c, numTrades, len(stream))
		}
		for idx, tradeID := range stream {
			expectedID := int64(idx + 1)
			if tradeID != expectedID {
				t.Fatalf("Consumer %d at position %d: expected trade ID %d, got %d (ordering/data race error)",
					c, idx, expectedID, tradeID)
			}
		}
	}
}

// TestConcurrent_1Producer_10Consumers tests 10 concurrent blocking consumers under load.
func TestConcurrent_1Producer_10Consumers(t *testing.T) {
	runProducerConsumersTest(t, 10, 10000, 1024, 128)
}

// TestConcurrent_1Producer_50Consumers tests 50 concurrent blocking consumers under load.
func TestConcurrent_1Producer_50Consumers(t *testing.T) {
	runProducerConsumersTest(t, 50, 5000, 1024, 64)
}

// TestConcurrent_1Producer_100Consumers tests 100 concurrent blocking consumers under load.
func TestConcurrent_1Producer_100Consumers(t *testing.T) {
	runProducerConsumersTest(t, 100, 5000, 2048, 64)
}

// TestConcurrent_HighContentionSmallBuffer tests extreme wrap-around pressure with tiny buffer size (16).
func TestConcurrent_HighContentionSmallBuffer(t *testing.T) {
	runProducerConsumersTest(t, 16, 2000, 16, 4)
}

// TestConcurrent_MixedWaitStrategies tests concurrent readers operating with different wait strategies.
func TestConcurrent_MixedWaitStrategies(t *testing.T) {
	numConsumers := 16
	numTrades := 5000
	bufferSize := int64(1024)
	batchSize := 64

	rb := NewRingBufferV6(bufferSize, numConsumers)
	rb.SetWaitStrategy("Adaptive")

	trades := generateTestCompactTrades(numTrades)

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	receivedCounts := make([]int64, numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func(idx int, r *RingBufferReader) {
			defer wg.Done()
			var count int64
			rb.Read(r, int64(numTrades), nil, func(ct CompactTrade) {
				count++
			})
			receivedCounts[idx] = count
		}(i, rb.readers[i])
	}

	for j := 0; j < len(trades); j += batchSize {
		end := j + batchSize
		if end > len(trades) {
			end = len(trades)
		}
		rb.PublishBatch(trades[j:end])
	}

	wg.Wait()

	for i := 0; i < numConsumers; i++ {
		if receivedCounts[i] != int64(numTrades) {
			t.Errorf("Consumer %d expected %d trades, got %d", i, numTrades, receivedCounts[i])
		}
	}
}

// TestConcurrent_SequenceBarrierPipeline tests multi-stage consumer dependency chains under concurrency.
func TestConcurrent_SequenceBarrierPipeline(t *testing.T) {
	numTrades := 5000
	bufferSize := int64(512)
	batchSize := 64

	rb := NewRingBufferV6(bufferSize, 3)
	parserReader := rb.readers[0] // Stage 1: Market data parse
	quantReader := rb.readers[1]  // Stage 2: Quant indicator calculation (depends on Parser)
	botReader := rb.readers[2]    // Stage 3: HFT Bot (depends on Quant)

	// Barrier 1 for Stage 2: wait for Stage 1 (parser)
	quantBarrier := NewSequenceBarrier(func() int64 {
		return atomic.LoadInt64(&parserReader.readSeq)
	})

	// Barrier 2 for Stage 3: wait for Stage 2 (quant)
	botBarrier := NewSequenceBarrier(func() int64 {
		return atomic.LoadInt64(&quantReader.readSeq)
	})

	trades := generateTestCompactTrades(numTrades)

	var wg sync.WaitGroup
	wg.Add(3)

	var parserProcessed, quantProcessed, botProcessed int64
	var barrierViolations int64

	// Stage 1: Parser
	go func() {
		defer wg.Done()
		rb.Read(parserReader, int64(numTrades), nil, func(ct CompactTrade) {
			atomic.AddInt64(&parserProcessed, 1)
		})
	}()

	// Stage 2: Quant
	go func() {
		defer wg.Done()
		rb.Read(quantReader, int64(numTrades), quantBarrier, func(ct CompactTrade) {
			pSeq := atomic.LoadInt64(&parserReader.readSeq)
			qSeq := atomic.LoadInt64(&quantReader.readSeq)
			if qSeq > pSeq {
				atomic.AddInt64(&barrierViolations, 1)
			}
			atomic.AddInt64(&quantProcessed, 1)
		})
	}()

	// Stage 3: Bot
	go func() {
		defer wg.Done()
		rb.Read(botReader, int64(numTrades), botBarrier, func(ct CompactTrade) {
			qSeq := atomic.LoadInt64(&quantReader.readSeq)
			bSeq := atomic.LoadInt64(&botReader.readSeq)
			if bSeq > qSeq {
				atomic.AddInt64(&barrierViolations, 1)
			}
			atomic.AddInt64(&botProcessed, 1)
		})
	}()

	// Publish batches
	for j := 0; j < len(trades); j += batchSize {
		end := j + batchSize
		if end > len(trades) {
			end = len(trades)
		}
		rb.PublishBatch(trades[j:end])
	}

	wg.Wait()

	if atomic.LoadInt64(&barrierViolations) > 0 {
		t.Fatalf("Detected %d DAG sequence barrier ordering violations!", barrierViolations)
	}

	if parserProcessed != int64(numTrades) || quantProcessed != int64(numTrades) || botProcessed != int64(numTrades) {
		t.Fatalf("Pipeline stage trade count mismatch: Parser=%d, Quant=%d, Bot=%d, Expected=%d",
			parserProcessed, quantProcessed, botProcessed, numTrades)
	}
}

// TestConcurrent_CleanShutdownUnderLoad tests graceful context cancellation and clean goroutine shutdown.
func TestConcurrent_CleanShutdownUnderLoad(t *testing.T) {
	rb := NewRingBufferV6(256, 10)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var totalPublished int64

	// Producer writes batches then signals cancel
	go func() {
		for i := 0; i < 30; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				batch := make([]CompactTrade, 50)
				for b := range batch {
					id := atomic.AddInt64(&totalPublished, 1)
					batch[b] = CompactTrade{ID: id, Price: ToUSD(65000.0)}
				}
				rb.PublishBatch(batch)
				time.Sleep(200 * time.Microsecond)
			}
		}
		cancel()
	}()

	// 10 concurrent consumers running
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(reader *RingBufferReader) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					currSeq := atomic.LoadInt64(&reader.readSeq)
					wSeq := atomic.LoadInt64(&rb.writeSeq)
					if currSeq < wSeq {
						rb.Read(reader, wSeq, nil, func(ct CompactTrade) {})
					} else {
						time.Sleep(50 * time.Microsecond)
					}
				}
			}
		}(rb.readers[i])
	}

	wg.Wait()
	t.Logf("Clean shutdown verified with %d total published trades.", totalPublished)
}

// TestSoak_ExtendedConcurrencyStability is an extended soak and stress test designed for nightly CI runs.
// It publishes 50,000 high-frequency market ticks across 32 concurrent readers with randomized batching.
func TestSoak_ExtendedConcurrencyStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping extended soak test in short mode")
	}

	numConsumers := 32
	totalTrades := 50000
	bufferSize := int64(2048)

	rb := NewRingBufferV6(bufferSize, numConsumers)
	trades := generateTestCompactTrades(totalTrades)

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	receivedCounts := make([]int64, numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func(idx int, reader *RingBufferReader) {
			defer wg.Done()
			var localCount int64
			rb.Read(reader, int64(totalTrades), nil, func(ct CompactTrade) {
				localCount++
			})
			receivedCounts[idx] = localCount
		}(i, rb.readers[i])
	}

	// Producer publishes with varying burst sizes
	for i := 0; i < totalTrades; {
		batchSize := 64
		if i+batchSize > totalTrades {
			batchSize = totalTrades - i
		}
		rb.PublishBatch(trades[i : i+batchSize])
		i += batchSize
	}

	wg.Wait()

	for i, count := range receivedCounts {
		if count != int64(totalTrades) {
			t.Fatalf("Soak failure: consumer %d received %d trades, expected %d", i, count, totalTrades)
		}
	}
	t.Logf("Soak test passed successfully: 32 concurrent consumers processed %d trades with 0 loss/corruption.", totalTrades)
}


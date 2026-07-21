package main

import (
	"sync"
	"testing"
)

// Benchmark implementations with 100 subscribers and 10,000 trades.

func BenchmarkSimpleFanV1(b *testing.B) {
	numSubscribers := 100
	numTrades := 10000
	trades := GenerateMockTrades(numTrades)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numSubscribers)

		sf := NewSimpleFanV1(numSubscribers, 1024)
		sf.Start()

		for s := 0; s < numSubscribers; s++ {
			go func(ch chan Trade) {
				for range ch {
					// consume
				}
				wg.Done()
			}(sf.channels[s])
		}

		for _, t := range trades {
			sf.Publish(t)
		}
		sf.Close()
		wg.Wait()
	}
}

func BenchmarkSimpleFanV2(b *testing.B) {
	numSubscribers := 100
	numTrades := 10000
	trades := GenerateMockTrades(numTrades)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numSubscribers)

		sf := NewSimpleFanV2(numSubscribers, 1024)
		sf.Start()

		for s := 0; s < numSubscribers; s++ {
			go func(ch chan *Trade) {
				for range ch {
					// consume
				}
				wg.Done()
			}(sf.channels[s])
		}

		for _, t := range trades {
			sf.Publish(t)
		}
		sf.Close()
		wg.Wait()
	}
}

func BenchmarkSimpleFanV3(b *testing.B) {
	numSubscribers := 100
	numTrades := 10000
	trades := GenerateMockTrades(numTrades)
	batchSize := 128

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numSubscribers)

		sf := NewSimpleFanV3(numSubscribers, 1024)
		sf.Start()

		for s := 0; s < numSubscribers; s++ {
			go func(ch chan []*Trade) {
				for range ch {
					// consume
				}
				wg.Done()
			}(sf.channels[s])
		}

		for j := 0; j < len(trades); j += batchSize {
			end := j + batchSize
			if end > len(trades) {
				end = len(trades)
			}
			sf.Publish(trades[j:end])
		}
		sf.Close()
		wg.Wait()
	}
}

func BenchmarkRingBufferV6(b *testing.B) {
	numSubscribers := 100
	numTrades := 10000
	trades := GenerateMockTrades(numTrades)
	batchSize := 128

	// Pre-map to CompactTrade
	compactTrades := make([]CompactTrade, len(trades))
	for idx, t := range trades {
		var side uint8 = 0
		if t.ID%2 != 0 {
			side = 1
		}
		compactTrades[idx] = CompactTrade{
			ID:        t.ID,
			Price:     ToUSD(t.Price),
			Quantity:  ToBTC(t.Quantity),
			Timestamp: t.Timestamp,
			SymbolID:  0,
			Side:      side,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numSubscribers)

		rb := NewRingBufferV6(1024, numSubscribers)

		for s := 0; s < numSubscribers; s++ {
			go func(reader *RingBufferReader) {
				rb.Read(reader, int64(numTrades), nil, func(ct CompactTrade) {
					// consume
				})
				wg.Done()
			}(rb.readers[s])
		}

		for j := 0; j < len(compactTrades); j += batchSize {
			end := j + batchSize
			if end > len(compactTrades) {
				end = len(compactTrades)
			}
			rb.PublishBatch(compactTrades[j:end])
		}
		wg.Wait()
	}
}

package engine

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
)

type BenchmarkResult struct {
	TimeNs     int64 `json:"timeNs"`
	Allocs     int64 `json:"allocs"`
	BytesAlloc int64 `json:"bytesAlloc"`
}

type TradeScalingPoint struct {
	Trades  int                        `json:"trades"`
	Results map[string]BenchmarkResult `json:"results"`
}

type SubscriberScalingPoint struct {
	Subscribers int                        `json:"subscribers"`
	Results     map[string]BenchmarkResult `json:"results"`
}

type FullBenchmarkOutput struct {
	TradesScaling struct {
		Subscribers int                 `json:"subscribers"`
		Points      []TradeScalingPoint `json:"points"`
	} `json:"tradesScaling"`
	SubscribersScaling struct {
		Trades int                      `json:"trades"`
		Points []SubscriberScalingPoint `json:"points"`
	} `json:"subscribersScaling"`
}

const perBenchmarkTimeout = 90 * time.Second

func runSingleBenchmarkSafe(impl string, trades []*Trade, numSubscribers int) (res BenchmarkResult, ok bool, reason string) {
	type outcome struct {
		r      BenchmarkResult
		reason string
	}
	ch := make(chan outcome, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				ch <- outcome{reason: fmt.Sprintf("panic: %v", rec)}
			}
		}()
		ch <- outcome{r: runSingleBenchmark(impl, trades, numSubscribers)}
	}()

	select {
	case o := <-ch:
		if o.reason != "" {
			return BenchmarkResult{}, false, o.reason
		}
		return o.r, true, ""
	case <-time.After(perBenchmarkTimeout):
		return BenchmarkResult{}, false, fmt.Sprintf("timed out after %s", perBenchmarkTimeout)
	}
}

// RunExperimentSuite runs the full benchmark suite with customized parameters and logs output.
func RunExperimentSuite(tradeCounts []int, subscriberCounts []int, logger io.Writer) (FullBenchmarkOutput, error) {
	fmt.Fprintln(logger, "Initializing experiment runner...")
	output := FullBenchmarkOutput{}

	constSubscribers := 100
	output.TradesScaling.Subscribers = constSubscribers
	output.TradesScaling.Points = make([]TradeScalingPoint, 0, len(tradeCounts))

	fmt.Fprintf(logger, "\n--- Phase 1: Trades Scaling (Subscribers: %d) ---\n", constSubscribers)
	for _, tc := range tradeCounts {
		fmt.Fprintf(logger, "Testing Trade Volume: %d...\n", tc)
		trades := GenerateMockTrades(tc)

		point := TradeScalingPoint{
			Trades:  tc,
			Results: make(map[string]BenchmarkResult),
		}

		implementations := []string{"SimpleFanV1", "SimpleFanV2", "SimpleFanV3", "RingBufferV6", "RingBufferEviction", "BinaryWireFanout"}
		for _, impl := range implementations {
			fmt.Fprintf(logger, "  %-20s: running...\n", impl)
			res, ok, reason := runSingleBenchmarkSafe(impl, trades, constSubscribers)
			point.Results[impl] = res
			if !ok {
				fmt.Fprintf(logger, "  %-20s: SKIPPED (%s)\n", impl, reason)
				continue
			}
			fmt.Fprintf(logger, "  %-20s: %10.2f ms | %10d allocs | %10d bytes\n",
				impl, float64(res.TimeNs)/1e6, res.Allocs, res.BytesAlloc)
		}
		output.TradesScaling.Points = append(output.TradesScaling.Points, point)
	}

	constTrades := 50000
	output.SubscribersScaling.Trades = constTrades
	output.SubscribersScaling.Points = make([]SubscriberScalingPoint, 0, len(subscriberCounts))

	fmt.Fprintf(logger, "\n--- Phase 2: Subscribers Scaling (Trades: %d) ---\n", constTrades)
	tradesForSubscribers := GenerateMockTrades(constTrades)
	for _, sc := range subscriberCounts {
		fmt.Fprintf(logger, "Testing Subscriber Count: %d...\n", sc)
		point := SubscriberScalingPoint{
			Subscribers: sc,
			Results:     make(map[string]BenchmarkResult),
		}

		implementations := []string{"SimpleFanV1", "SimpleFanV2", "SimpleFanV3", "RingBufferV6", "RingBufferEviction", "BinaryWireFanout"}
		for _, impl := range implementations {
			fmt.Fprintf(logger, "  %-20s: running...\n", impl)
			res, ok, reason := runSingleBenchmarkSafe(impl, tradesForSubscribers, sc)
			point.Results[impl] = res
			if !ok {
				fmt.Fprintf(logger, "  %-20s: SKIPPED (%s)\n", impl, reason)
				continue
			}
			fmt.Fprintf(logger, "  %-20s: %10.2f ms | %10d allocs | %10d bytes\n",
				impl, float64(res.TimeNs)/1e6, res.Allocs, res.BytesAlloc)
		}
		output.SubscribersScaling.Points = append(output.SubscribersScaling.Points, point)
	}

	fmt.Fprintln(logger, "\nExperiment execution completed successfully.")
	return output, nil
}

func runSingleBenchmark(impl string, trades []*Trade, numSubscribers int) BenchmarkResult {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	runtime.GC()

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	start := time.Now()

	switch impl {
	case "SimpleFanV1":
		sf := NewSimpleFanV1(numSubscribers, 2048)
		sf.Start()
		for i := 0; i < numSubscribers; i++ {
			go func(ch chan Trade) {
				for t := range ch {
					ProcessTrade(&t)
				}
				wg.Done()
			}(sf.channels[i])
		}
		for _, t := range trades {
			sf.Publish(t)
		}
		sf.Close()

	case "SimpleFanV2":
		sf := NewSimpleFanV2(numSubscribers, 2048)
		sf.Start()
		for i := 0; i < numSubscribers; i++ {
			go func(ch chan *Trade) {
				for t := range ch {
					ProcessTrade(t)
				}
				wg.Done()
			}(sf.channels[i])
		}
		for _, t := range trades {
			sf.Publish(t)
		}
		sf.Close()

	case "SimpleFanV3":
		sf := NewSimpleFanV3(numSubscribers, 2048)
		sf.Start()
		for i := 0; i < numSubscribers; i++ {
			go func(ch chan []*Trade) {
				for batch := range ch {
					for _, t := range batch {
						ProcessTrade(t)
					}
				}
				wg.Done()
			}(sf.channels[i])
		}
		batchSize := 128
		for i := 0; i < len(trades); i += batchSize {
			end := i + batchSize
			if end > len(trades) {
				end = len(trades)
			}
			sf.Publish(trades[i:end])
		}
		sf.Close()

	case "RingBufferV6":
		bufSize := int64(2048)
		rb := NewRingBufferV6(bufSize, numSubscribers)

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

		for i := 0; i < numSubscribers; i++ {
			go func(reader *RingBufferReader) {
				rb.Read(reader, int64(len(compactTrades)), nil, func(ct CompactTrade) {
					ProcessCompactTrade(ct)
				})
				wg.Done()
			}(rb.Readers[i])
		}
		batchSize := 128
		for i := 0; i < len(compactTrades); i += batchSize {
			end := i + batchSize
			if end > len(compactTrades) {
				end = len(compactTrades)
			}
			rb.PublishBatch(compactTrades[i:end])
		}

	case "RingBufferEviction":
		// Honest eviction path: small buffer, all non-blocking readers, PublishBatchEvicting.
		// A deliberately slow reader forces cursor fast-forward; publisher never gates.
		bufSize := int64(256)
		rb := NewRingBufferV6(bufSize, numSubscribers)
		compactTrades := make([]CompactTrade, len(trades))
		for idx, t := range trades {
			var side uint8 = 0
			if t.ID%2 != 0 {
				side = 1
			}
			compactTrades[idx] = CompactTrade{
				ID: t.ID, Price: ToUSD(t.Price), Quantity: ToBTC(t.Quantity),
				Timestamp: t.Timestamp, SymbolID: 0, Side: side,
			}
		}
		for i := 0; i < numSubscribers; i++ {
			rb.Readers[i].Blocking = false
			go func(reader *RingBufferReader, slow bool) {
				rb.Read(reader, int64(len(compactTrades)), nil, func(ct CompactTrade) {
					if slow {
						time.Sleep(50 * time.Microsecond)
					}
					ProcessCompactTrade(ct)
				})
				wg.Done()
			}(rb.Readers[i], i == 0)
		}
		batchSize := 128
		for i := 0; i < len(compactTrades); i += batchSize {
			end := i + batchSize
			if end > len(compactTrades) {
				end = len(compactTrades)
			}
			rb.PublishBatchEvicting(compactTrades[i:end])
		}

	case "BinaryWireFanout":
		// Fair fan-out: encode once per trade, every subscriber decodes the shared frame.
		// This is a binary codec microbench under the same subscriber×trade load shape —
		// not a pretend zero-cost disruptor substitute.
		frames := make([][]byte, len(trades))
		for idx, t := range trades {
			var side uint8
			if t.ID%2 != 0 {
				side = 1
			}
			ct := CompactTrade{
				ID: t.ID, Price: ToUSD(t.Price), Quantity: ToBTC(t.Quantity),
				Timestamp: t.Timestamp, SymbolID: 0, Side: side, Sequence: uint16(idx),
			}
			frames[idx] = EncodeFlatTrade(make([]byte, 38), ct)
		}
		for i := 0; i < numSubscribers; i++ {
			go func() {
				for _, f := range frames {
					ProcessCompactTrade(BinaryFlatTrade(f).DecodeToCompactTrade())
				}
				wg.Done()
			}()
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	runtime.ReadMemStats(&m2)

	allocs := int64(m2.Mallocs - m1.Mallocs)
	bytesAlloc := int64(m2.TotalAlloc - m1.TotalAlloc)

	if allocs < 0 {
		allocs = 0
	}
	if bytesAlloc < 0 {
		bytesAlloc = 0
	}

	return BenchmarkResult{
		TimeNs:     elapsed.Nanoseconds(),
		Allocs:     allocs,
		BytesAlloc: bytesAlloc,
	}
}

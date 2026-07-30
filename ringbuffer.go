package main

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// RingBufferReader represents a subscriber's state.
// Each subscriber tracks its own progress using readSeq atomically.
// Cache-line padding is added to prevent false sharing under high contention.
type RingBufferReader struct {
	id           int
	blocking     bool
	readSeq      int64    // atomic sequence pointer
	evictedCount int64    // atomic count of evictions/overruns for slow market data readers
	_            [39]byte // 39-byte padding to occupy a full 64-byte cache line (8+1+8+8+39 = 64)
}

// WaitStrategy defines the interface for consumer wait paths.
type WaitStrategy interface {
	WaitFor(targetSeq int64, currentSeqGetter func() int64) int64
	Signal()
}

// BlockingWaitStrategy utilizes sync.Cond for CPU-friendly blocking.
type BlockingWaitStrategy struct {
	cond   *sync.Cond
	condMu sync.Mutex
}

func NewBlockingWaitStrategy() *BlockingWaitStrategy {
	bws := &BlockingWaitStrategy{}
	bws.cond = sync.NewCond(&bws.condMu)
	return bws
}

func (s *BlockingWaitStrategy) WaitFor(targetSeq int64, currentSeqGetter func() int64) int64 {
	curr := currentSeqGetter()
	if curr >= targetSeq {
		return curr
	}
	s.condMu.Lock()
	for {
		curr = currentSeqGetter()
		if curr >= targetSeq {
			break
		}
		s.cond.Wait()
	}
	s.condMu.Unlock()
	return curr
}

func (s *BlockingWaitStrategy) Signal() {
	s.condMu.Lock()
	s.cond.Broadcast()
	s.condMu.Unlock()
}

// AdaptiveWaitStrategy spins, yields Gosched, then blocks to prevent thundering herd CPU spikes.
type AdaptiveWaitStrategy struct {
	cond   *sync.Cond
	condMu sync.Mutex
}

func NewAdaptiveWaitStrategy() *AdaptiveWaitStrategy {
	aws := &AdaptiveWaitStrategy{}
	aws.cond = sync.NewCond(&aws.condMu)
	return aws
}

func (s *AdaptiveWaitStrategy) WaitFor(targetSeq int64, currentSeqGetter func() int64) int64 {
	// Phase 1: Fast Spin (50 iterations)
	for i := 0; i < 50; i++ {
		curr := currentSeqGetter()
		if curr >= targetSeq {
			return curr
		}
	}
	// Phase 2: Yield CPU scheduler (50 iterations)
	for i := 0; i < 50; i++ {
		curr := currentSeqGetter()
		if curr >= targetSeq {
			return curr
		}
		runtime.Gosched()
	}
	// Phase 3: Block on condition variable
	s.condMu.Lock()
	var finalCurr int64
	for {
		finalCurr = currentSeqGetter()
		if finalCurr >= targetSeq {
			break
		}
		s.cond.Wait()
	}
	s.condMu.Unlock()
	return finalCurr
}

func (s *AdaptiveWaitStrategy) Signal() {
	s.condMu.Lock()
	s.cond.Signal() // Wakes 1 waiter at a time to prevent thundering herd
	s.condMu.Unlock()
}

// YieldingWaitStrategy spins up to 100 times before yielding CPU thread scheduling.
type YieldingWaitStrategy struct{}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{}
}

func (s *YieldingWaitStrategy) WaitFor(targetSeq int64, currentSeqGetter func() int64) int64 {
	counter := 0
	for {
		curr := currentSeqGetter()
		if curr >= targetSeq {
			return curr
		}
		counter++
		if counter > 100 {
			runtime.Gosched()
		}
	}
}

func (s *YieldingWaitStrategy) Signal() {
	// No-op for yield/spin strategies
}

// BusySpinWaitStrategy spins continuously in a hot CPU loop for sub-microsecond latency.
type BusySpinWaitStrategy struct{}

func NewBusySpinWaitStrategy() *BusySpinWaitStrategy {
	return &BusySpinWaitStrategy{}
}

func (s *BusySpinWaitStrategy) WaitFor(targetSeq int64, currentSeqGetter func() int64) int64 {
	for {
		curr := currentSeqGetter()
		if curr >= targetSeq {
			return curr
		}
	}
}

func (s *BusySpinWaitStrategy) Signal() {
	// No-op
}

// SequenceBarrier coordinates dependency chains between concurrent consumers.
type SequenceBarrier struct {
	dependentSeqs []func() int64
}

func NewSequenceBarrier(getters ...func() int64) *SequenceBarrier {
	return &SequenceBarrier{dependentSeqs: getters}
}

func (sb *SequenceBarrier) GetAvailableSequence() int64 {
	if len(sb.dependentSeqs) == 0 {
		return 0
	}
	min := sb.dependentSeqs[0]()
	for i := 1; i < len(sb.dependentSeqs); i++ {
		seq := sb.dependentSeqs[i]()
		if seq < min {
			min = seq
		}
	}
	return min
}

// RingBufferV6 implements a lock-free, zero-allocation power-of-2 circular buffer.
// It stores pointerless CompactTrade structs by value to eliminate GC scan overhead entirely.
type RingBufferV6 struct {
	buffer       []CompactTrade
	size         int64
	mask         int64

	_            [56]byte // Padding to isolate writeSeq
	writeSeq     int64    // atomic write sequence
	_            [56]byte // Padding to isolate cachedMin
	cachedMin    int64    // atomic cached minimum reader sequence
	_            [56]byte // Padding

	readers      []*RingBufferReader
	waitStrategy WaitStrategy
}

// NewRingBufferV6 creates a new RingBufferV6 with size buffer slots and N readers.
func NewRingBufferV6(size int64, numSubscribers int) *RingBufferV6 {
	// Size must be power of 2
	if (size & (size - 1)) != 0 {
		panic("Size must be a power of 2")
	}

	rb := &RingBufferV6{
		buffer:       make([]CompactTrade, size),
		size:         size,
		mask:         size - 1,
		waitStrategy: NewBlockingWaitStrategy(), // default strategy
	}

	rb.readers = make([]*RingBufferReader, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		rb.readers[i] = &RingBufferReader{
			id:       i,
			blocking: true,
			readSeq:  0,
		}
	}

	return rb
}

// SetWaitStrategy changes the wait strategy at runtime.
func (rb *RingBufferV6) SetWaitStrategy(strategy string) {
	switch strategy {
	case "Yielding":
		rb.waitStrategy = NewYieldingWaitStrategy()
	case "BusySpin":
		rb.waitStrategy = NewBusySpinWaitStrategy()
	case "Adaptive":
		rb.waitStrategy = NewAdaptiveWaitStrategy()
	default:
		rb.waitStrategy = NewBlockingWaitStrategy()
	}
}

// getMinReaderSeq returns the minimum sequence read by any blocking subscriber.
func (rb *RingBufferV6) getMinReaderSeq() int64 {
	writeSeq := atomic.LoadInt64(&rb.writeSeq)
	min := writeSeq
	hasBlocking := false
	for _, r := range rb.readers {
		if r.blocking {
			seq := atomic.LoadInt64(&r.readSeq)
			if !hasBlocking || seq < min {
				min = seq
				hasBlocking = true
			}
		}
	}
	return min
}

// PublishBatch writes a batch of Trades to the RingBuffer by value copy.
func (rb *RingBufferV6) PublishBatch(trades []CompactTrade) {
	seq := rb.writeSeq

	for _, t := range trades {
		// Wrap-around check: cannot overwrite if we'd overtake slowest consumer
		for {
			cachedMin := atomic.LoadInt64(&rb.cachedMin)
			if seq-cachedMin < rb.size {
				break // Safe to write
			}

			// Cache expired or we are wrapping, check actual slowest reader
			minSeq := rb.getMinReaderSeq()
			atomic.StoreInt64(&rb.cachedMin, minSeq)

			if seq-minSeq < rb.size {
				break // Safe to write
			}

			// Yield to let readers progress
			runtime.Gosched()
		}

		idx := seq & rb.mask
		rb.buffer[idx] = t // Direct value copy! No sync.Pool or pointer recycling!
		seq++
	}

	// Update the write pointer atomically so readers see the batch
	atomic.StoreInt64(&rb.writeSeq, seq)

	// Notify waiting readers
	rb.waitStrategy.Signal()
}

// PublishBatchEvicting writes trades to the RingBuffer without ever blocking.
// Non-blocking market data mode: slow consumers who fall behind are automatically evicted/overrun.
func (rb *RingBufferV6) PublishBatchEvicting(trades []CompactTrade) {
	seq := atomic.LoadInt64(&rb.writeSeq)

	for _, t := range trades {
		idx := seq & rb.mask
		rb.buffer[idx] = t
		seq++
	}

	atomic.StoreInt64(&rb.writeSeq, seq)

	// Check if non-blocking readers have fallen behind and evict/skip them forward
	for _, r := range rb.readers {
		if !r.blocking {
			rSeq := atomic.LoadInt64(&r.readSeq)
			if seq-rSeq > rb.size {
				// Reader was overrun! Fast forward reader sequence to oldest valid slot in buffer
				atomic.StoreInt64(&r.readSeq, seq-rb.size)
				atomic.AddInt64(&r.evictedCount, 1)
			}
		}
	}

	rb.waitStrategy.Signal()
}

// Read runs the reader loop, processing up to targetCount total trades using a sequence barrier.
func (rb *RingBufferV6) Read(reader *RingBufferReader, targetCount int64, barrier *SequenceBarrier, process func(CompactTrade)) {
	readSeq := int64(0)

	var limitGetter func() int64
	if barrier == nil {
		limitGetter = func() int64 {
			return atomic.LoadInt64(&rb.writeSeq)
		}
	} else {
		limitGetter = func() int64 {
			return barrier.GetAvailableSequence()
		}
	}

	for readSeq < targetCount {
		limitSeq := limitGetter()

		if readSeq >= limitSeq {
			// Queue is empty or blocked by dependency barrier, wait
			limitSeq = rb.waitStrategy.WaitFor(readSeq+1, limitGetter)
		}

		// Check if reader was overrun in non-blocking mode
		if !reader.blocking {
			currentRSeq := atomic.LoadInt64(&reader.readSeq)
			if currentRSeq > readSeq {
				readSeq = currentRSeq // Catch up to skipped position
			}
		}

		// Read available trades in a batch
		for readSeq < limitSeq && readSeq < targetCount {
			idx := readSeq & rb.mask
			trade := rb.buffer[idx]
			process(trade)
			readSeq++
		}

		// Mark our progress atomically
		atomic.StoreInt64(&reader.readSeq, readSeq)
	}
}


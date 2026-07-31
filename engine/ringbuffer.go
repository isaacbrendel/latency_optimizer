package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// RingBufferReader represents a subscriber's state.
type RingBufferReader struct {
	ID           int   `json:"id"`
	Blocking     bool  `json:"blocking"`
	ReadSeq      int64 `json:"readSeq"`      // atomic sequence pointer
	EvictedCount int64 `json:"evictedCount"` // atomic count of evictions/overruns for slow market data readers
	_            [39]byte
}

func (r *RingBufferReader) SetReadSeq(seq int64) {
	atomic.StoreInt64(&r.ReadSeq, seq)
}

func (r *RingBufferReader) GetReadSeq() int64 {
	return atomic.LoadInt64(&r.ReadSeq)
}

func (r *RingBufferReader) GetEvictedCount() int64 {
	return atomic.LoadInt64(&r.EvictedCount)
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
	for i := 0; i < 50; i++ {
		curr := currentSeqGetter()
		if curr >= targetSeq {
			return curr
		}
	}
	for i := 0; i < 50; i++ {
		curr := currentSeqGetter()
		if curr >= targetSeq {
			return curr
		}
		runtime.Gosched()
	}
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
	s.cond.Signal()
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

func (s *YieldingWaitStrategy) Signal() {}

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

func (s *BusySpinWaitStrategy) Signal() {}

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
type RingBufferV6 struct {
	Buffer       []CompactTrade
	Size         int64
	Mask         int64

	_            [56]byte
	WriteSeq     int64
	_            [56]byte
	CachedMin    int64
	_            [56]byte

	Readers      []*RingBufferReader
	WaitStrategy WaitStrategy
}

// NewRingBufferV6 creates a new RingBufferV6 with size buffer slots and N readers.
func NewRingBufferV6(size int64, numSubscribers int) *RingBufferV6 {
	if (size & (size - 1)) != 0 {
		panic("Size must be a power of 2")
	}

	rb := &RingBufferV6{
		Buffer:       make([]CompactTrade, size),
		Size:         size,
		Mask:         size - 1,
		WaitStrategy: NewBlockingWaitStrategy(),
	}

	rb.Readers = make([]*RingBufferReader, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		rb.Readers[i] = &RingBufferReader{
			ID:       i,
			Blocking: true,
			ReadSeq:  0,
		}
	}

	return rb
}

func (rb *RingBufferV6) SetWaitStrategy(strategy string) {
	switch strategy {
	case "Yielding":
		rb.WaitStrategy = NewYieldingWaitStrategy()
	case "BusySpin":
		rb.WaitStrategy = NewBusySpinWaitStrategy()
	case "Adaptive":
		rb.WaitStrategy = NewAdaptiveWaitStrategy()
	default:
		rb.WaitStrategy = NewBlockingWaitStrategy()
	}
}

func (rb *RingBufferV6) GetWriteSeq() int64 {
	return atomic.LoadInt64(&rb.WriteSeq)
}

func (rb *RingBufferV6) GetMinReaderSeq() int64 {
	writeSeq := atomic.LoadInt64(&rb.WriteSeq)
	min := writeSeq
	hasBlocking := false
	for _, r := range rb.Readers {
		if r.Blocking {
			seq := atomic.LoadInt64(&r.ReadSeq)
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
	seq := rb.WriteSeq

	for _, t := range trades {
		for {
			cachedMin := atomic.LoadInt64(&rb.CachedMin)
			if seq-cachedMin < rb.Size {
				break
			}

			minSeq := rb.GetMinReaderSeq()
			atomic.StoreInt64(&rb.CachedMin, minSeq)

			if seq-minSeq < rb.Size {
				break
			}

			runtime.Gosched()
		}

		idx := seq & rb.Mask
		rb.Buffer[idx] = t
		seq++
	}

	atomic.StoreInt64(&rb.WriteSeq, seq)
	rb.WaitStrategy.Signal()
}

// PublishBatchEvicting writes trades to the RingBuffer without ever blocking.
func (rb *RingBufferV6) PublishBatchEvicting(trades []CompactTrade) {
	seq := atomic.LoadInt64(&rb.WriteSeq)

	for _, t := range trades {
		idx := seq & rb.Mask
		rb.Buffer[idx] = t
		seq++
	}

	atomic.StoreInt64(&rb.WriteSeq, seq)

	for _, r := range rb.Readers {
		if !r.Blocking {
			rSeq := atomic.LoadInt64(&r.ReadSeq)
			if seq-rSeq > rb.Size {
				atomic.StoreInt64(&r.ReadSeq, seq-rb.Size)
				atomic.AddInt64(&r.EvictedCount, 1)
			}
		}
	}

	rb.WaitStrategy.Signal()
}

// Read runs the reader loop, processing up to targetCount total trades using a sequence barrier.
func (rb *RingBufferV6) Read(reader *RingBufferReader, targetCount int64, barrier *SequenceBarrier, process func(CompactTrade)) {
	readSeq := int64(0)

	var limitGetter func() int64
	if barrier == nil {
		limitGetter = func() int64 {
			return atomic.LoadInt64(&rb.WriteSeq)
		}
	} else {
		limitGetter = func() int64 {
			return barrier.GetAvailableSequence()
		}
	}

	for readSeq < targetCount {
		limitSeq := limitGetter()

		if readSeq >= limitSeq {
			limitSeq = rb.WaitStrategy.WaitFor(readSeq+1, limitGetter)
		}

		if !reader.Blocking {
			currentRSeq := atomic.LoadInt64(&reader.ReadSeq)
			if currentRSeq > readSeq {
				readSeq = currentRSeq
			}
		}

		for readSeq < limitSeq && readSeq < targetCount {
			idx := readSeq & rb.Mask
			trade := rb.Buffer[idx]
			process(trade)
			readSeq++
		}

		atomic.StoreInt64(&reader.ReadSeq, readSeq)
	}
}

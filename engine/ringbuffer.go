package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// cacheLinePad isolates atomic cursors onto separate 64-byte cache lines so
// writers and readers do not invalidate each other's L1/L2 lines (false sharing).
type cacheLinePad struct{ _ [64]byte }

// RingBufferReader is a single-writer cursor owned by one consumer goroutine.
// ReadSeq / EvictedCount are the only cross-goroutine fields and are always
// accessed atomically.
type RingBufferReader struct {
	ID           int
	Blocking     bool
	pad0         cacheLinePad
	ReadSeq      int64 // atomic
	pad1         cacheLinePad
	EvictedCount int64 // atomic
	pad2         cacheLinePad
}

func (r *RingBufferReader) SetReadSeq(seq int64) { atomic.StoreInt64(&r.ReadSeq, seq) }
func (r *RingBufferReader) GetReadSeq() int64    { return atomic.LoadInt64(&r.ReadSeq) }
func (r *RingBufferReader) GetEvictedCount() int64 {
	return atomic.LoadInt64(&r.EvictedCount)
}

// WaitStrategy wakes consumers when new sequences are published.
type WaitStrategy interface {
	WaitFor(targetSeq int64, currentSeqGetter func() int64) int64
	Signal()
}

// BlockingWaitStrategy parks on sync.Cond (lowest CPU, highest wake latency).
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

// AdaptiveWaitStrategy: spin → Gosched → block.
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
		if curr := currentSeqGetter(); curr >= targetSeq {
			return curr
		}
	}
	for i := 0; i < 50; i++ {
		if curr := currentSeqGetter(); curr >= targetSeq {
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
	s.cond.Broadcast()
	s.condMu.Unlock()
}

// YieldingWaitStrategy spins then yields; never blocks the OS thread via Cond.
type YieldingWaitStrategy struct{}

func NewYieldingWaitStrategy() *YieldingWaitStrategy { return &YieldingWaitStrategy{} }

func (s *YieldingWaitStrategy) WaitFor(targetSeq int64, currentSeqGetter func() int64) int64 {
	counter := 0
	for {
		if curr := currentSeqGetter(); curr >= targetSeq {
			return curr
		}
		counter++
		if counter > 100 {
			runtime.Gosched()
		}
	}
}

func (s *YieldingWaitStrategy) Signal() {}

// BusySpinWaitStrategy burns a core for minimum wake latency.
type BusySpinWaitStrategy struct{}

func NewBusySpinWaitStrategy() *BusySpinWaitStrategy { return &BusySpinWaitStrategy{} }

func (s *BusySpinWaitStrategy) WaitFor(targetSeq int64, currentSeqGetter func() int64) int64 {
	for {
		if curr := currentSeqGetter(); curr >= targetSeq {
			return curr
		}
	}
}

func (s *BusySpinWaitStrategy) Signal() {}

// SequenceBarrier coordinates consumer dependency DAGs without locks.
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
		if seq := sb.dependentSeqs[i](); seq < min {
			min = seq
		}
	}
	return min
}

// slot is a race-free ring entry. Payload fields are stored as atomics; the
// published sequence is stored last (release) and re-checked after loads
// (acquire) so torn reads are rejected without data races under the Go memory model.
type slot struct {
	id        int64 // atomic
	price     int64 // atomic USD
	qty       int64 // atomic BTC
	ts        int64 // atomic
	meta      int64 // atomic: seq16|sym8|side8|flags8|venue8 in low bytes
	published int64 // atomic: absolute ring sequence that owns this slot; -1 = empty
}

func packMeta(t CompactTrade) int64 {
	return int64(t.Sequence) |
		int64(t.SymbolID)<<16 |
		int64(t.Side)<<24 |
		int64(t.Flags)<<32 |
		int64(t.VenueID)<<40
}

func unpackMeta(m int64, t *CompactTrade) {
	t.Sequence = uint16(m & 0xffff)
	t.SymbolID = uint8((m >> 16) & 0xff)
	t.Side = uint8((m >> 24) & 0xff)
	t.Flags = uint8((m >> 32) & 0xff)
	t.VenueID = uint8((m >> 40) & 0xff)
}

func (s *slot) store(seq int64, t CompactTrade) {
	atomic.StoreInt64(&s.id, t.ID)
	atomic.StoreInt64(&s.price, int64(t.Price))
	atomic.StoreInt64(&s.qty, int64(t.Quantity))
	atomic.StoreInt64(&s.ts, t.Timestamp)
	atomic.StoreInt64(&s.meta, packMeta(t))
	// Publish last — readers that observe this sequence see a complete payload.
	atomic.StoreInt64(&s.published, seq)
}

func (s *slot) load(expectedSeq int64) (CompactTrade, bool) {
	if atomic.LoadInt64(&s.published) != expectedSeq {
		return CompactTrade{}, false
	}
	var t CompactTrade
	t.ID = atomic.LoadInt64(&s.id)
	t.Price = USD(atomic.LoadInt64(&s.price))
	t.Quantity = BTC(atomic.LoadInt64(&s.qty))
	t.Timestamp = atomic.LoadInt64(&s.ts)
	unpackMeta(atomic.LoadInt64(&s.meta), &t)
	// Tear check: publisher may have overwritten mid-read.
	if atomic.LoadInt64(&s.published) != expectedSeq {
		return CompactTrade{}, false
	}
	return t, true
}

func (s *slot) snapshot() CompactTrade {
	var t CompactTrade
	t.ID = atomic.LoadInt64(&s.id)
	t.Price = USD(atomic.LoadInt64(&s.price))
	t.Quantity = BTC(atomic.LoadInt64(&s.qty))
	t.Timestamp = atomic.LoadInt64(&s.ts)
	unpackMeta(atomic.LoadInt64(&s.meta), &t)
	return t
}

// RingBufferV6 is a single-producer, multi-consumer power-of-two disruptor.
// Concurrent PublishBatch calls are rejected (single-writer principle).
type RingBufferV6 struct {
	slots []slot
	Size  int64
	Mask  int64

	padW       cacheLinePad
	WriteSeq   int64 // atomic — next sequence to publish (exclusive end)
	padC       cacheLinePad
	CachedMin  int64 // atomic — cached min gating reader cursor
	padP       cacheLinePad
	publishing int32 // atomic — single-writer guard
	padR       cacheLinePad

	Readers      []*RingBufferReader
	WaitStrategy WaitStrategy

	// Buffer mirrors slot payloads for API introspection (race-free snapshots).
	// Prefer SlotAt / SnapshotSlots for concurrent access.
	Buffer []CompactTrade
}


// NewRingBufferV6 creates a ring with power-of-two capacity and numSubscribers readers.
func NewRingBufferV6(size int64, numSubscribers int) *RingBufferV6 {
	if size <= 0 || (size&(size-1)) != 0 {
		panic("Size must be a power of 2")
	}
	if numSubscribers < 0 {
		panic("numSubscribers must be >= 0")
	}

	rb := &RingBufferV6{
		slots:        make([]slot, size),
		Buffer:       make([]CompactTrade, size),
		Size:         size,
		Mask:         size - 1,
		WaitStrategy: NewBlockingWaitStrategy(),
	}
	for i := range rb.slots {
		atomic.StoreInt64(&rb.slots[i].published, -1)
	}

	rb.Readers = make([]*RingBufferReader, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		rb.Readers[i] = &RingBufferReader{ID: i, Blocking: true, ReadSeq: 0}
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

func (rb *RingBufferV6) GetWriteSeq() int64 { return atomic.LoadInt64(&rb.WriteSeq) }

func (rb *RingBufferV6) beginPublish() {
	if !atomic.CompareAndSwapInt32(&rb.publishing, 0, 1) {
		panic("RingBufferV6: concurrent publish violates single-writer principle")
	}
}

func (rb *RingBufferV6) endPublish() { atomic.StoreInt32(&rb.publishing, 0) }

// GetMinReaderSeq returns the slowest *gating* (blocking) reader cursor.
// Non-blocking readers never gate the producer.
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

func (rb *RingBufferV6) hasGatingReaders() bool {
	for _, r := range rb.Readers {
		if r.Blocking {
			return true
		}
	}
	return false
}

func (rb *RingBufferV6) waitForGatingCapacity(seq int64) {
	// No blocking consumers ⇒ lossy overwrite is allowed; never stall the publisher.
	if !rb.hasGatingReaders() {
		return
	}
	for {
		cachedMin := atomic.LoadInt64(&rb.CachedMin)
		if seq-cachedMin < rb.Size {
			return
		}
		minSeq := rb.GetMinReaderSeq()
		atomic.StoreInt64(&rb.CachedMin, minSeq)
		if seq-minSeq < rb.Size {
			return
		}
		runtime.Gosched()
	}
}

func (rb *RingBufferV6) evictSlowReaders(newEndSeq int64) {
	for _, r := range rb.Readers {
		if r.Blocking {
			continue
		}
		for {
			rSeq := atomic.LoadInt64(&r.ReadSeq)
			if newEndSeq-rSeq <= rb.Size {
				break
			}
			// Fast-forward past the soon-to-be-overwritten region.
			target := newEndSeq - rb.Size
			if atomic.CompareAndSwapInt64(&r.ReadSeq, rSeq, target) {
				atomic.AddInt64(&r.EvictedCount, 1)
				break
			}
		}
	}
}

func (rb *RingBufferV6) writeSlot(seq int64, t CompactTrade) {
	idx := seq & rb.Mask
	rb.slots[idx].store(seq, t)
	rb.Buffer[idx] = t // mirror for dashboards; slot atomics are the source of truth
}

// PublishBatch gates on blocking readers, then publishes. Non-blocking readers
// that fall more than Size behind are evicted (cursor advanced) before overwrite.
func (rb *RingBufferV6) PublishBatch(trades []CompactTrade) {
	if len(trades) == 0 {
		return
	}
	rb.beginPublish()
	defer rb.endPublish()

	seq := atomic.LoadInt64(&rb.WriteSeq)
	for _, t := range trades {
		rb.waitForGatingCapacity(seq)
		rb.evictSlowReaders(seq + 1)
		rb.writeSlot(seq, t)
		seq++
	}
	atomic.StoreInt64(&rb.WriteSeq, seq)
	rb.WaitStrategy.Signal()
}

// PublishBatchEvicting never waits on readers. Blocking readers that would be
// overrun are forcibly advanced (same as non-blocking) so the producer never
// stalls — suitable for lossy market-data fan-out, not for audit trails.
func (rb *RingBufferV6) PublishBatchEvicting(trades []CompactTrade) {
	if len(trades) == 0 {
		return
	}
	rb.beginPublish()
	defer rb.endPublish()

	seq := atomic.LoadInt64(&rb.WriteSeq)
	newEnd := seq + int64(len(trades))

	// Evict ANY reader that would be overwritten — including blocking ones.
	for _, r := range rb.Readers {
		for {
			rSeq := atomic.LoadInt64(&r.ReadSeq)
			if newEnd-rSeq <= rb.Size {
				break
			}
			target := newEnd - rb.Size
			if atomic.CompareAndSwapInt64(&r.ReadSeq, rSeq, target) {
				atomic.AddInt64(&r.EvictedCount, 1)
				break
			}
		}
	}

	for _, t := range trades {
		rb.writeSlot(seq, t)
		seq++
	}
	atomic.StoreInt64(&rb.WriteSeq, seq)
	rb.WaitStrategy.Signal()
}

// SlotAt returns a race-free snapshot of ring index i (mod Size).
func (rb *RingBufferV6) SlotAt(i int64) CompactTrade {
	return rb.slots[i&rb.Mask].snapshot()
}

// TryLoadSequence loads the event published at absolute sequence `seq`.
func (rb *RingBufferV6) TryLoadSequence(seq int64) (CompactTrade, bool) {
	return rb.slots[seq&rb.Mask].load(seq)
}

// Read processes events until the reader's cursor reaches targetCount.
// targetCount is an absolute sequence end (same as historical API: process
// sequences [0, targetCount)).
func (rb *RingBufferV6) Read(reader *RingBufferReader, targetCount int64, barrier *SequenceBarrier, process func(CompactTrade)) {
	readSeq := atomic.LoadInt64(&reader.ReadSeq)

	limitGetter := func() int64 {
		if barrier == nil {
			return atomic.LoadInt64(&rb.WriteSeq)
		}
		return barrier.GetAvailableSequence()
	}

	for readSeq < targetCount {
		// External eviction / test harness may advance the cursor concurrently.
		if cur := atomic.LoadInt64(&reader.ReadSeq); cur > readSeq {
			readSeq = cur
			continue
		}
		if readSeq >= targetCount {
			break
		}

		limitSeq := limitGetter()
		if readSeq >= limitSeq {
			limitSeq = rb.WaitStrategy.WaitFor(readSeq+1, func() int64 {
				if cur := atomic.LoadInt64(&reader.ReadSeq); cur > readSeq {
					return cur
				}
				return limitGetter()
			})
		}

		// Absorb producer-side eviction.
		if cur := atomic.LoadInt64(&reader.ReadSeq); cur > readSeq {
			readSeq = cur
			continue
		}

		if !reader.Blocking {
			if w := atomic.LoadInt64(&rb.WriteSeq); w-readSeq > rb.Size {
				readSeq = w - rb.Size
				atomic.StoreInt64(&reader.ReadSeq, readSeq)
				atomic.AddInt64(&reader.EvictedCount, 1)
			}
		}

		for readSeq < limitSeq && readSeq < targetCount {
			if cur := atomic.LoadInt64(&reader.ReadSeq); cur > readSeq {
				readSeq = cur
				break
			}
			if !reader.Blocking {
				if w := atomic.LoadInt64(&rb.WriteSeq); w-readSeq > rb.Size {
					readSeq = w - rb.Size
					atomic.StoreInt64(&reader.ReadSeq, readSeq)
					atomic.AddInt64(&reader.EvictedCount, 1)
					break
				}
			}

			trade, ok := rb.slots[readSeq&rb.Mask].load(readSeq)
			if !ok {
				// Slot overwritten or not yet visible — resync cursor.
				if !reader.Blocking {
					w := atomic.LoadInt64(&rb.WriteSeq)
					if w-readSeq > rb.Size {
						readSeq = w - rb.Size
						rb.advanceReader(reader, &readSeq)
						atomic.AddInt64(&reader.EvictedCount, 1)
					}
				}
				runtime.Gosched()
				break
			}
			process(trade)
			readSeq++
			rb.advanceReader(reader, &readSeq)
		}
		rb.WaitStrategy.Signal()
	}
}

// advanceReader publishes the local cursor unless an external fast-forward already
// moved it ahead (eviction / shutdown). Never clobber a higher ReadSeq.
func (rb *RingBufferV6) advanceReader(reader *RingBufferReader, readSeq *int64) {
	for {
		cur := atomic.LoadInt64(&reader.ReadSeq)
		if cur > *readSeq {
			*readSeq = cur
			return
		}
		if atomic.CompareAndSwapInt64(&reader.ReadSeq, cur, *readSeq) {
			return
		}
	}
}

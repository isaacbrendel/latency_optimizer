package main

import (
	"sync"
)

// ==========================================
// SimpleFanV1: Copies 80-byte Trade struct
// ==========================================

type SimpleFanV1 struct {
	in       chan Trade
	channels []chan Trade
	wg       sync.WaitGroup
	done     chan struct{}
}

func NewSimpleFanV1(numSubscribers int, bufferSize int) *SimpleFanV1 {
	channels := make([]chan Trade, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		channels[i] = make(chan Trade, bufferSize)
	}
	return &SimpleFanV1{
		in:       make(chan Trade, bufferSize),
		channels: channels,
		done:     make(chan struct{}),
	}
}

func (sf *SimpleFanV1) Start() {
	go func() {
		for trade := range sf.in {
			for _, ch := range sf.channels {
				select {
				case ch <- trade:
				default:
					// Drop or block. For benchmarking, we block to ensure delivery.
					ch <- trade
				}
			}
		}
		for _, ch := range sf.channels {
			close(ch)
		}
		close(sf.done)
	}()
}

func (sf *SimpleFanV1) Publish(t *Trade) {
	sf.in <- CopyTrade(t) // Value copy
}

func (sf *SimpleFanV1) Close() {
	close(sf.in)
	<-sf.done
}

// ==========================================
// SimpleFanV2: Passes *Trade pointer (8 bytes)
// ==========================================

type SimpleFanV2 struct {
	in       chan *Trade
	channels []chan *Trade
	done     chan struct{}
}

func NewSimpleFanV2(numSubscribers int, bufferSize int) *SimpleFanV2 {
	channels := make([]chan *Trade, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		channels[i] = make(chan *Trade, bufferSize)
	}
	return &SimpleFanV2{
		in:       make(chan *Trade, bufferSize),
		channels: channels,
		done:     make(chan struct{}),
	}
}

func (sf *SimpleFanV2) Start() {
	go func() {
		for trade := range sf.in {
			for _, ch := range sf.channels {
				ch <- trade
			}
		}
		for _, ch := range sf.channels {
			close(ch)
		}
		close(sf.done)
	}()
}

func (sf *SimpleFanV2) Publish(t *Trade) {
	sf.in <- t
}

func (sf *SimpleFanV2) Close() {
	close(sf.in)
	<-sf.done
}

// ==========================================
// SimpleFanV3: Batched pointer fan-out
// ==========================================

type SimpleFanV3 struct {
	in       chan []*Trade
	channels []chan []*Trade
	done     chan struct{}
}

func NewSimpleFanV3(numSubscribers int, bufferSize int) *SimpleFanV3 {
	channels := make([]chan []*Trade, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		channels[i] = make(chan []*Trade, bufferSize)
	}
	return &SimpleFanV3{
		in:       make(chan []*Trade, bufferSize),
		channels: channels,
		done:     make(chan struct{}),
	}
}

func (sf *SimpleFanV3) Start() {
	go func() {
		for batch := range sf.in {
			for _, ch := range sf.channels {
				ch <- batch
			}
		}
		for _, ch := range sf.channels {
			close(ch)
		}
		close(sf.done)
	}()
}

func (sf *SimpleFanV3) Publish(batch []*Trade) {
	sf.in <- batch
}

func (sf *SimpleFanV3) Close() {
	close(sf.in)
	<-sf.done
}

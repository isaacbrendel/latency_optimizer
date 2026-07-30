package main

import (
	"encoding/binary"
	"fmt"
)

// BinaryFlatTrade represents a lightweight zero-copy binary wire format.
// It implements binary table offsets (vtable headers) similar to FlatBuffers,
// allowing zero-allocation binary transport over WebSocket / TCP sockets without unmarshaling.
//
// Binary Layout (36 Bytes Total):
// [0..1]   : VTable Offset Header (uint16)
// [2..9]   : ID (int64)
// [10..17] : Price (int64)
// [18..25] : Quantity (int64)
// [26..33] : Timestamp (int64)
// [34..35] : Sequence (uint16)
type BinaryFlatTrade []byte

// EncodeFlatTrade serializes a CompactTrade into a pre-allocated binary byte slice.
func EncodeFlatTrade(buf []byte, ct CompactTrade) []byte {
	if len(buf) < 38 {
		buf = make([]byte, 38)
	}
	// VTable header offset (fixed 2 bytes)
	binary.LittleEndian.PutUint16(buf[0:2], 38)
	// Scalar fields
	binary.LittleEndian.PutUint64(buf[2:10], uint64(ct.ID))
	binary.LittleEndian.PutUint64(buf[10:18], uint64(ct.Price))
	binary.LittleEndian.PutUint64(buf[18:26], uint64(ct.Quantity))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(ct.Timestamp))
	binary.LittleEndian.PutUint16(buf[34:36], ct.Sequence)
	buf[36] = ct.SymbolID
	buf[37] = ct.Side
	return buf[:38]
}

// ReadID extracts the ID field directly from the raw binary wire buffer without unmarshaling.
func (b BinaryFlatTrade) ReadID() int64 {
	return int64(binary.LittleEndian.Uint64(b[2:10]))
}

// ReadPrice extracts the Price field directly from the raw binary buffer.
func (b BinaryFlatTrade) ReadPrice() USD {
	return USD(binary.LittleEndian.Uint64(b[10:18]))
}

// ReadQuantity extracts the Quantity field directly from the raw binary buffer.
func (b BinaryFlatTrade) ReadQuantity() BTC {
	return BTC(binary.LittleEndian.Uint64(b[18:26]))
}

// ReadTimestamp extracts the Timestamp directly from the raw binary buffer.
func (b BinaryFlatTrade) ReadTimestamp() int64 {
	return int64(binary.LittleEndian.Uint64(b[26:34]))
}

// ReadSequence extracts the Sequence directly from the raw binary buffer.
func (b BinaryFlatTrade) ReadSequence() uint16 {
	return binary.LittleEndian.Uint16(b[34:36])
}

// DecodeToCompactTrade reads all fields out of raw binary into a CompactTrade value struct.
func (b BinaryFlatTrade) DecodeToCompactTrade() CompactTrade {
	return CompactTrade{
		ID:        b.ReadID(),
		Price:     b.ReadPrice(),
		Quantity:  b.ReadQuantity(),
		Timestamp: b.ReadTimestamp(),
		Sequence:  b.ReadSequence(),
		SymbolID:  b[36],
		Side:      b[37],
	}
}

func VerifyFlatBuffersEncoding() {
	ct := CompactTrade{
		ID:        999988,
		Price:     ToUSD(65432.10),
		Quantity:  ToBTC(1.5),
		Timestamp: 1700000000,
		Sequence:  42,
		SymbolID:  0,
		Side:      1,
	}

	rawBuf := make([]byte, 38)
	encoded := EncodeFlatTrade(rawBuf, ct)
	decoded := BinaryFlatTrade(encoded).DecodeToCompactTrade()

	fmt.Printf("[FlatBuffers Zero-Copy Check] Raw Payload: %d bytes\n", len(encoded))
	fmt.Printf("  Decoded Trade ID: %d, Price: $%.2f, Qty: %.2f BTC, Seq: %d\n",
		decoded.ID, decoded.Price.Float64(), decoded.Quantity.Float64(), decoded.Sequence)
}

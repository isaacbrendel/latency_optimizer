package engine

import (
	"encoding/binary"
	"fmt"
)

// BinaryFlatTrade represents a lightweight zero-copy binary wire format.
type BinaryFlatTrade []byte

// EncodeFlatTrade serializes a CompactTrade into a pre-allocated binary byte slice.
func EncodeFlatTrade(buf []byte, ct CompactTrade) []byte {
	if len(buf) < 38 {
		buf = make([]byte, 38)
	}
	binary.LittleEndian.PutUint16(buf[0:2], 38)
	binary.LittleEndian.PutUint64(buf[2:10], uint64(ct.ID))
	binary.LittleEndian.PutUint64(buf[10:18], uint64(ct.Price))
	binary.LittleEndian.PutUint64(buf[18:26], uint64(ct.Quantity))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(ct.Timestamp))
	binary.LittleEndian.PutUint16(buf[34:36], ct.Sequence)
	buf[36] = ct.SymbolID
	buf[37] = ct.Side
	return buf[:38]
}

func (b BinaryFlatTrade) ReadID() int64 {
	return int64(binary.LittleEndian.Uint64(b[2:10]))
}

func (b BinaryFlatTrade) ReadPrice() USD {
	return USD(binary.LittleEndian.Uint64(b[10:18]))
}

func (b BinaryFlatTrade) ReadQuantity() BTC {
	return BTC(binary.LittleEndian.Uint64(b[18:26]))
}

func (b BinaryFlatTrade) ReadTimestamp() int64 {
	return int64(binary.LittleEndian.Uint64(b[26:34]))
}

func (b BinaryFlatTrade) ReadSequence() uint16 {
	return binary.LittleEndian.Uint16(b[34:36])
}

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

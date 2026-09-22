package engine

import (
	"encoding/binary"
	"fmt"
)

// BinaryFlatTrade is a fixed 38-byte little-endian wire frame for CompactTrade.
// Named historically; it is a hand-rolled binary layout (not FlatBuffers IDL).
type BinaryFlatTrade []byte

const BinaryWireFrameSize = 38

// EncodeFlatTrade serializes a CompactTrade into a pre-allocated binary frame.
func EncodeFlatTrade(buf []byte, ct CompactTrade) []byte {
	if len(buf) < BinaryWireFrameSize {
		buf = make([]byte, BinaryWireFrameSize)
	}
	binary.LittleEndian.PutUint16(buf[0:2], BinaryWireFrameSize)
	binary.LittleEndian.PutUint64(buf[2:10], uint64(ct.ID))
	binary.LittleEndian.PutUint64(buf[10:18], uint64(ct.Price))
	binary.LittleEndian.PutUint64(buf[18:26], uint64(ct.Quantity))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(ct.Timestamp))
	binary.LittleEndian.PutUint16(buf[34:36], ct.Sequence)
	buf[36] = ct.SymbolID
	buf[37] = ct.Side
	return buf[:BinaryWireFrameSize]
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

// VerifyBinaryWireEncoding round-trips a sample trade (debug helper).
func VerifyBinaryWireEncoding() {
	ct := CompactTrade{
		ID: 999988, Price: ToUSD(65432.10), Quantity: ToBTC(1.5),
		Timestamp: 1700000000, Sequence: 42, SymbolID: 0, Side: 1,
	}
	encoded := EncodeFlatTrade(make([]byte, BinaryWireFrameSize), ct)
	decoded := BinaryFlatTrade(encoded).DecodeToCompactTrade()
	fmt.Printf("[Binary Wire] %d bytes id=%d price=$%.2f\n",
		len(encoded), decoded.ID, decoded.Price.Float64())
}

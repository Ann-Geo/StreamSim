package helpers

import (
	"dstream-sim/constants"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

func Int64ToBytesLE(n int64) ([]byte, error) {
	buf := make([]byte, 8)
	endianness := DetermineEndianness()
	if endianness == constants.LITTLE_ENDIAN {
		binary.LittleEndian.PutUint64(buf, uint64(n))
	} else if endianness == constants.BIG_ENDIAN {
		binary.BigEndian.PutUint64(buf, uint64(n))
	} else {
		return nil, fmt.Errorf("unsupported endianness: %s", endianness)
	}
	return buf, nil
}

func BytesToInt64LE(b []byte) (int64, error) {
	if len(b) < 8 {
		err := fmt.Errorf("byte slice too short to convert to int64")
		return 0, err
	}

	endianness := DetermineEndianness()
	var u uint64
	if endianness == constants.LITTLE_ENDIAN {
		u = binary.LittleEndian.Uint64(b)
	} else if endianness == constants.BIG_ENDIAN {
		u = binary.BigEndian.Uint64(b)
	} else {
		return 0, fmt.Errorf("unsupported endianness: %v", endianness)
	}

	return int64(u), nil
}

func DetermineEndianness() string {
	var i uint16 = 0x1
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, i)
	var endianness string

	if bs[0] == 0x1 {
		endianness = constants.LITTLE_ENDIAN
	} else {
		endianness = constants.BIG_ENDIAN
	}

	return endianness
}

func Float64FromInt64Bits(bits int64) float64 {
	return math.Float64frombits(uint64(bits))
}

func TimeDurationToString(durations []time.Duration) string {
	strParts := make([]string, len(durations))
	for i, d := range durations {
		strParts[i] = d.String() // e.g., "500ms", "2s"
	}
	return strings.Join(strParts, ",")
}

func TimeDurationsToByte(durations []time.Duration) []byte {
	strs := make([]string, len(durations))
	for i, d := range durations {
		strs[i] = d.String()
	}
	return []byte(strings.Join(strs, ","))
}

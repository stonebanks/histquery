package helpers

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

func ToNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func ToNullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

func Float32sToBytes(v []float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

func BytesToFloat32s(v []byte) ([]float32, error) {
	if len(v)%4 != 0 {
		return nil, fmt.Errorf("vector byte length %d is not a multiple of 4", len(v))
	}
	aArr := make([]float32, len(v)/4)
	for i := range aArr {
		aArr[i] = BytesFloat32(v[i*4:])
	}
	return aArr, nil
}

func BytesFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

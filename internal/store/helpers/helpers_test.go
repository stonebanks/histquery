package helpers

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToNullString(t *testing.T) {
	assert.Equal(t, sql.NullString{String: "", Valid: false}, ToNullString(""))
	assert.Equal(t, sql.NullString{String: "hello", Valid: true}, ToNullString("hello"))
}

func TestToNullTime(t *testing.T) {
	assert.Equal(t, sql.NullTime{Valid: false}, ToNullTime(time.Time{}))

	now := time.Now()
	assert.Equal(t, sql.NullTime{Time: now, Valid: true}, ToNullTime(now))
}

func TestFloat32sToBytes(t *testing.T) {
	v := []float32{1.5, -2.25, 0}
	b := Float32sToBytes(v)
	assert.Len(t, b, len(v)*4)

	got, err := BytesToFloat32s(b)
	require.NoError(t, err)
	assert.Equal(t, v, got)
}

func TestBytesToFloat32s_InvalidLength(t *testing.T) {
	_, err := BytesToFloat32s([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestBytesFloat32(t *testing.T) {
	want := float32(3.14)
	b := Float32sToBytes([]float32{want})
	assert.Equal(t, want, BytesFloat32(b))
}

package util

import (
	"testing"
)

func BenchmarkGUID(b *testing.B) {
	var okays, errors, fails int64
	var previd uid
	factory, _ := NewSonwflacke(1)
	for i := 0; i < b.N; i++ {
		id := factory.Getid()
		if id == previd {
			fails++
			b.Fail()
		} else {
			okays++
		}
		b.Logf("bytes %s", id.Tobytes())
	}
	b.Logf("okays=%d errors=%d bads=%d", okays, errors, fails)
}

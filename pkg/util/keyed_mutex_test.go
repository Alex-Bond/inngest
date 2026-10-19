package util

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyedMutexSerialisesSameKey(t *testing.T) {
	k := NewKeyedMutex()
	var inside, maxInside atomic.Int32
	wg := sync.WaitGroup{}
	for range 50 {
		wg.Go(func() {
			unlock := k.Lock([]byte("same"))
			defer unlock()
			n := inside.Add(1)
			for {
				m := maxInside.Load()
				if n <= m || maxInside.CompareAndSwap(m, n) {
					break
				}
			}
			inside.Add(-1)
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, maxInside.Load())
}

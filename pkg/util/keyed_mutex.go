package util

import (
	"hash/maphash"
	"sync"
)

const keyedMutexStripes = 256

// KeyedMutex serialises work per key within a process using a fixed set of
// striped mutexes; unrelated keys may share a stripe.
type KeyedMutex struct {
	seed    maphash.Seed
	stripes [keyedMutexStripes]sync.Mutex
}

func NewKeyedMutex() *KeyedMutex {
	return &KeyedMutex{seed: maphash.MakeSeed()}
}

// Lock locks the stripe for key and returns its unlock function.
func (k *KeyedMutex) Lock(key []byte) func() {
	m := &k.stripes[maphash.Bytes(k.seed, key)%keyedMutexStripes]
	m.Lock()
	return m.Unlock
}

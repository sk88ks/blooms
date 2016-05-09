package blooms

import (
	"bytes"
	"encoding/gob"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

var defaultHasher = murmur3.New64()

// baseFilter is base for variety of filters
type baseFilter struct {
	mu sync.RWMutex
	// Bit map slice
	bits []uint8
	// Number of hash functions
	k int
	// Number of elements
	n int
	// Number of element per a slice
	s int
}

// baseGobs is gob stream receiver
type baseGobs struct {
	Bits []uint8
	K    int
	N    int
	S    int
}

// gobEncode encodes filter to gob stream
func gobEncode(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// gobDecode encodes filter to gob stream
func gobDecode(data []byte, dst interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(dst)
	if err != nil {
		return err
	}
	return nil
}

func divideHash(h uint64) (h1 uint32, h2 uint32) {
	// Get first half                                                                                                x
	h1 = uint32(h >> 32)
	// Get later half
	h2 = uint32(h & ((1 << 32) - 1))
	return
}

func getIndex(h1, h2 uint32, i, size int) int {
	return int(h1+uint32(i)*h2) % size
}

// createHash creats 64bit hash
func (b *baseFilter) createHash(element []byte) uint64 {
	hasher := defaultHasher
	hasher.Reset()
	hasher.Write(element)
	return hasher.Sum64()
}

// Add adds a new element into bloomfilter
func (b *baseFilter) Add(element []byte) {
	h := b.createHash(element)
	h1, h2 := divideHash(h)
	size := len(b.bits)
	// For partitioned filter
	if b.s != 0 {
		size = b.s
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < b.k; i++ {
		idx := getIndex(h1, h2, i, size) + (i * b.s)
		// Increment counter up to 255
		if b.bits[idx] < 0xFF {
			b.bits[idx]++
		}
	}
	b.n++
}

// Has checks if a element already exists in bit map
func (b *baseFilter) Has(element []byte) bool {
	h := b.createHash(element)
	h1, h2 := divideHash(h)
	size := len(b.bits)
	// For partitioned filter
	if b.s != 0 {
		size = b.s
	}
	for i := 0; i < b.k; i++ {
		idx := getIndex(h1, h2, i, size) + (i * b.s)
		if b.bits[idx] == 0 {
			return false
		}
	}
	return true
}

func (b *baseFilter) toGobs() *baseGobs {
	return &baseGobs{
		Bits: b.bits,
		K:    b.k,
		N:    b.n,
		S:    b.s,
	}
}

func (b *baseGobs) toFilter() *baseFilter {
	return &baseFilter{
		bits: b.Bits,
		k:    b.K,
		n:    b.N,
		s:    b.S,
	}
}

// GobEncode encodes data to gobs stream
func (b *baseFilter) GobEncode() ([]byte, error) {
	data := b.toGobs()
	return gobEncode(data)
}

// BloomFilter is basic implementation of bloomfilter
type BloomFilter struct {
	*baseFilter
}

// New creates a new bloomfilter instance
func New(filterSize, hasherNumber int) *BloomFilter {
	return &BloomFilter{
		&baseFilter{
			bits: make([]uint8, filterSize),
			k:    hasherNumber,
		},
	}
}

// GetFalsePositiveIncidence gets the incidence of false positive
func (b *BloomFilter) GetFalsePositiveIncidence() float64 {
	return math.Pow((1 - math.Exp(float64(-b.k*b.n)/float64(len(b.bits)))), float64(b.k))
}

// GobDecode decodes gob stream
func (b *BloomFilter) GobDecode(data []byte) error {
	var bg baseGobs
	err := gobDecode(data, &bg)
	if err != nil {
		return err
	}

	b.baseFilter = bg.toFilter()
	return nil
}

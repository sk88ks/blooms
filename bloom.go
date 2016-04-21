package blooms

import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

var defaultHasher = murmur3.New64()

// baseFilter is base for variety of filters
type baseFilter struct {
	bits []uint8
	// Number of hash functions
	k int
	// Number of elements
	n int
	// Number of element per a slice
	s int
	// hasher is hash function
	hasher hash.Hash64
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
	hasher := b.hasher
	if hasher == nil {
		hasher = defaultHasher
	}
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
	for i := 0; i < b.k; i++ {
		idx := getIndex(h1, h2, i, size) + (i * b.s)
		b.bits[idx] = 1
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

// BloomFilter is basic implementation of bloomfilter
type BloomFilter struct {
	*baseFilter
}

// New creates a new bloomfilter instance
func New(filterSize, hasherNumber int, hasher hash.Hash64) *BloomFilter {
	return &BloomFilter{
		&baseFilter{
			bits:   make([]uint8, filterSize),
			k:      hasherNumber,
			hasher: hasher,
		},
	}
}

// GetFalsePositiveIncidence gets the incidence of false positive
func (b *BloomFilter) GetFalsePositiveIncidence() float64 {
	return math.Pow((1 - math.Exp(float64(-b.k*b.n)/float64(len(b.bits)))), float64(b.k))
}

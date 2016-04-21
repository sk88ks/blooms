package blooms

import "hash"

// CountingFilter is implementation of countable bloomfilter
// This supports counting with uint8 bit map which increment its counter
// up to uint8 max size
type CountingFilter struct {
	*baseFilter
}

// NewCountingFilter creates a new cuntable bloomfilter instance
func NewCountingFilter(filterSize, hasherNumber int, hasher hash.Hash64) *CountingFilter {
	return &CountingFilter{
		&baseFilter{
			bits:   make([]uint8, filterSize),
			k:      hasherNumber,
			hasher: hasher,
		},
	}
}

// Add adds a new element into bloomfilter
func (c *CountingFilter) Add(element []byte) {
	h := c.createHash(element)
	h1, h2 := divideHash(h)
	for i := 0; i < c.k; i++ {
		idx := getIndex(h1, h2, i, len(c.bits))
		// Increment counter up to 255
		if c.bits[idx] < 0xFF {
			c.bits[idx]++
		}
	}
	c.n++
}

// Remove removes a element from counting filter
func (c *CountingFilter) Remove(element []byte) {
	h := c.createHash(element)
	h1, h2 := divideHash(h)
	for i := 0; i < c.k; i++ {
		idx := getIndex(h1, h2, i, len(c.bits))
		if c.bits[idx] > 0 {
			c.bits[idx]--
		}
	}
	c.n--
}

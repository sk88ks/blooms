package blooms

// CountingFilter is implementation of countable bloomfilter
// This supports counting with uint8 bit map which increment its counter
// up to uint8 max size
type CountingFilter struct {
	*baseFilter
}

// NewCountingFilter creates a new cuntable bloomfilter instance
func NewCountingFilter(filterSize, hasherNumber int) *CountingFilter {
	return &CountingFilter{
		&baseFilter{
			bits: make([]uint8, filterSize),
			k:    hasherNumber,
		},
	}
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

// GobDecode decodes gob stream
func (c *CountingFilter) GobDecode(data []byte) error {
	var bg baseGobs
	err := gobDecode(data, &bg)
	if err != nil {
		return err
	}

	c.baseFilter = bg.toFilter()
	return nil
}

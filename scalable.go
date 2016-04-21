package blooms

import (
	"hash"
	"math"
)

// PartitionedFilter is implementation of partitioned bloomfilter
type PartitionedFilter struct {
	*baseFilter
	// Max number of elements
	maxN int
	// Expected incidence of flase positive as origin
	p float64
}

// NewPartitionedFilter creates a new partitioned bloomfilter instance
func NewPartitionedFilter(filterSize, hasherNumber int, hasher hash.Hash64) *PartitionedFilter {
	return &PartitionedFilter{
		baseFilter: &baseFilter{
			bits:   make([]uint8, filterSize),
			k:      hasherNumber,
			hasher: hasher,
			s:      int(filterSize / hasherNumber),
		},
	}
}

// PartitionedFilters is slice of PartitionedFilter
type PartitionedFilters []*PartitionedFilter

// Last returns the last element of PartitionedFilters
func (ps PartitionedFilters) Last() *PartitionedFilter {
	return ps[len(ps)-1]
}

// ScalableFilter is implementation of scalsble bloomfilter
type ScalableFilter struct {
	filters PartitionedFilters
	// Number of hash functions as origin
	k int
	// Filter size as origin
	m int
	// Number of elements in all filters
	n int64
	// Max number of elements in every filter
	maxN int
	// Expected incidence of flase positive as origin
	p float64
	// Growth rate for a new filter size by a previous one
	growthRate int
	// Reduction rate of false positive incidence
	fpReduction float64
	// hasher is hash function
	hasher hash.Hash64
}

// NewScalableFilter creates a new scalable bloomfilter instance
func NewScalableFilter(filterSize, growthRate int, expectedFP, fpReduction float64, hasher hash.Hash64) *ScalableFilter {
	sf := &ScalableFilter{
		m:           filterSize,
		p:           expectedFP,
		growthRate:  growthRate,
		fpReduction: fpReduction,
		hasher:      hasher,
	}

	// Set origin expected false positive instance
	sf.k = GetMinimumHasherNumber(expectedFP)

	sf.addFilter()

	return sf
}

// addFilter append a new filter
func (sf *ScalableFilter) addFilter() {
	// Filters growth number
	growthNum := float64(len(sf.filters))
	filterSize := sf.m * int(math.Pow(float64(sf.growthRate), growthNum))
	expectedFP := sf.p * math.Pow(sf.fpReduction, float64(len(sf.filters)))
	hasherNumber := sf.k + int(growthNum*math.Log2(1/sf.fpReduction)+1)
	pf := NewPartitionedFilter(filterSize, hasherNumber, sf.hasher)
	pf.maxN = GetBestElementNumber(filterSize, expectedFP)
	pf.p = expectedFP
	sf.filters = append(sf.filters, pf)
}

// Add adds a new element into filter and return false if couldn't do it.
// In case false positive incidence is bigger than expected,
// create a new filter and set element into it.
func (sf *ScalableFilter) Add(element []byte) {
	if sf.filters.Last().n >= sf.filters.Last().maxN {
		sf.addFilter()
	}

	sf.filters.Last().Add(element)

	sf.n++
}

// Has checks whether a element already exists in all filters
func (sf *ScalableFilter) Has(element []byte) bool {
	for i := len(sf.filters) - 1; i >= 0; i-- {
		if sf.filters[i].Has(element) {
			return true
		}
	}
	return false
}

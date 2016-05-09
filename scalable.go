package blooms

import "math"

// PartitionedFilter is implementation of partitioned bloomfilter
type PartitionedFilter struct {
	*baseFilter
	// Max number of elements
	maxN int
	// Expected incidence of flase positive as origin
	p float64
}

type partitionedGobs struct {
	Base *baseGobs
	MaxN int
	P    float64
}

// NewPartitionedFilter creates a new partitioned bloomfilter instance
func NewPartitionedFilter(filterSize, hasherNumber int) *PartitionedFilter {
	return &PartitionedFilter{
		baseFilter: &baseFilter{
			bits: make([]uint8, filterSize),
			k:    hasherNumber,
			s:    int(filterSize / hasherNumber),
		},
	}
}

func (p *PartitionedFilter) toGobs() *partitionedGobs {
	return &partitionedGobs{
		Base: p.baseFilter.toGobs(),
		MaxN: p.maxN,
		P:    p.p,
	}
}

func (p *partitionedGobs) toFilter() *PartitionedFilter {
	return &PartitionedFilter{
		baseFilter: p.Base.toFilter(),
		maxN:       p.MaxN,
		p:          p.P,
	}
}

// PartitionedFilters is slice of PartitionedFilter
type PartitionedFilters []*PartitionedFilter

type partitionedGobsSet []*partitionedGobs

// Last returns the last element of PartitionedFilters
func (ps PartitionedFilters) Last() *PartitionedFilter {
	return ps[len(ps)-1]
}

func (ps PartitionedFilters) toGobs() []*partitionedGobs {
	pgs := make([]*partitionedGobs, len(ps))
	for i := range ps {
		pgs[i] = ps[i].toGobs()
	}
	return pgs
}

func (pgs partitionedGobsSet) toFilters() PartitionedFilters {
	ps := make(PartitionedFilters, len(pgs))
	for i := range pgs {
		ps[i] = pgs[i].toFilter()
	}
	return ps
}

// GobEncode encodes data to gobs stream
func (p *PartitionedFilter) GobEncode() ([]byte, error) {
	data := p.toGobs()
	return gobEncode(data)
}

// GobDecode decodes gob stream to filter
func (p *PartitionedFilter) GobDecode(data []byte) error {
	var pg partitionedGobs
	err := gobDecode(data, &pg)
	if err != nil {
		return err
	}

	p.baseFilter = pg.Base.toFilter()
	p.maxN = pg.MaxN
	p.p = pg.P
	return nil
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
}

type scalableGobs struct {
	Filters     partitionedGobsSet
	K           int
	M           int
	N           int64
	MaxN        int
	P           float64
	GrowthRate  int
	FpReduction float64
}

// NewScalableFilter creates a new scalable bloomfilter instance
func NewScalableFilter(filterSize, growthRate int, expectedFP, fpReduction float64) *ScalableFilter {
	sf := &ScalableFilter{
		m:           filterSize,
		p:           expectedFP,
		growthRate:  growthRate,
		fpReduction: fpReduction,
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
	pf := NewPartitionedFilter(filterSize, hasherNumber)
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

func (sf *ScalableFilter) toGobs() *scalableGobs {
	return &scalableGobs{
		Filters:     sf.filters.toGobs(),
		K:           sf.k,
		M:           sf.m,
		N:           sf.n,
		MaxN:        sf.maxN,
		P:           sf.p,
		GrowthRate:  sf.growthRate,
		FpReduction: sf.fpReduction,
	}
}

// GobEncode encodes data to gob stream
func (sf *ScalableFilter) GobEncode() ([]byte, error) {
	data := sf.toGobs()
	return gobEncode(data)
}

// GobDecode decodes gobs stream
func (sf *ScalableFilter) GobDecode(data []byte) error {
	var sg scalableGobs
	err := gobDecode(data, &sg)
	if err != nil {
		return err
	}

	sf.filters = sg.Filters.toFilters()
	sf.k = sg.K
	sf.m = sg.M
	sf.n = sg.N
	sf.maxN = sg.MaxN
	sf.p = sg.P
	sf.growthRate = sg.GrowthRate
	sf.fpReduction = sg.FpReduction

	return nil
}

package blooms

import (
	"testing"

	"github.com/satori/go.uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewPartitionedFilter(t *testing.T) {
	Convey("Given filter size, hasher number", t, func() {
		m := 128
		k := 5

		Convey("When creating a new bloom filter", func() {
			b := NewPartitionedFilter(m, k)

			Convey("Then created instance should be expected", func() {
				So(b, ShouldNotBeNil)
				So(len(b.bits), ShouldEqual, m)
				So(b.k, ShouldEqual, k)
				So(b.s, ShouldEqual, int(m/k))

			})
		})
	})
}

func TestPartitionedFilter_GobEncode(t *testing.T) {
	Convey("Given bloom filter", t, func() {
		m := 128
		k := 2

		p := NewPartitionedFilter(m, k)

		e := []byte("test")
		p.Add(e)

		Convey("When converting gobs stream", func() {
			buf, err := p.GobEncode()

			Convey("Then expected bytes slice should be returned", func() {
				So(err, ShouldBeNil)
				So(len(buf), ShouldNotEqual, 0)

			})
		})
	})
}

func TestPartitionedFilter_GobDecode(t *testing.T) {
	Convey("Given bloom filter converted to gobs stream", t, func() {
		m := 128
		k := 2

		p := NewPartitionedFilter(m, k)

		e := []byte("test")
		p.Add(e)

		buf, _ := p.GobEncode()

		Convey("When decoding gobs stream", func() {
			res := &PartitionedFilter{}
			err := res.GobDecode(buf)

			Convey("Then expected bytes slice should be returned", func() {
				So(err, ShouldBeNil)
				So(len(res.bits), ShouldEqual, 128)
				So(res.k, ShouldEqual, p.k)
				So(res.s, ShouldEqual, p.s)
				So(res.Has([]byte("test")), ShouldBeTrue)

			})
		})
	})
}

func TestNewScalableFilter(t *testing.T) {
	Convey("Given filter size, growth rate, reduction, fp and hasher", t, func() {
		m := 128
		gr := 2
		var fp, reduction float64
		fp = 0.0001
		reduction = 0.8

		Convey("When creating a new scalable bloom filter", func() {
			b := NewScalableFilter(m, gr, fp, reduction)

			Convey("Then created instance should be expected", func() {
				So(b, ShouldNotBeNil)
				So(b.k, ShouldEqual, 13)
				So(len(b.filters), ShouldEqual, 1)
				So(b.filters[0].maxN, ShouldEqual, 6)

			})
		})
	})
}

func TestScalableFilter_Add(t *testing.T) {
	Convey("Given scalable bloom filter", t, func() {
		m := 128
		gr := 2
		var fp, reduction float64
		fp = 0.0001
		reduction = 0.8

		b := NewScalableFilter(m, gr, fp, reduction)

		Convey("When add new elements", func() {
			e := []byte("test")
			b.Add(e)

			Convey("Then element should be added", func() {
				So(len(b.filters), ShouldEqual, 1)
				var count int
				previous := 0
				current := b.filters[0].s
				for i := range b.filters[0].bits {
					if b.filters[0].bits[i] == 1 {
						So(i, ShouldBeBetweenOrEqual, previous, current)
						previous = current
						current += b.filters[0].s
						count++
					}
				}
				So(count, ShouldEqual, b.filters[0].k)

			})
		})
	})

	Convey("Given scalable bloom filter for large data set", t, func() {
		m := 128
		gr := 2
		var fp, reduction float64
		fp = 0.0001
		reduction = 0.8

		b := NewScalableFilter(m, gr, fp, reduction)

		Convey("When add many elements", func() {
			e := []byte("test")
			for i := 0; i < 10000; i++ {
				elm := append(e, byte(i))
				b.Add(elm)
			}

			Convey("Then element should be added", func() {
				So(len(b.filters), ShouldEqual, 11)
				So(b.filters.Last().k, ShouldEqual, 17)
				So(b.filters.Last().maxN, ShouldEqual, 5503)
				So(len(b.filters.Last().bits), ShouldEqual, 131072)

			})
		})
	})
}

func TestScalableFilter_Has(t *testing.T) {
	Convey("Given scalable bloom filter", t, func() {
		gr := 2
		var fp, reduction float64
		fp = 0.001
		reduction = 0.5
		m := GetBestFilterSize(15000, fp)

		b := NewScalableFilter(m, gr, fp, reduction)

		var firstOne []byte
		for i := 0; i < 10000; i++ {
			elm := uuid.NewV4().Bytes()
			if i == 0 {
				firstOne = elm
			}
			b.Add(elm)
		}

		Convey("When check a element", func() {
			check := b.Has(firstOne)

			Convey("Then element should be added", func() {
				So(check, ShouldBeTrue)

			})
		})

		Convey("When check unset elements", func() {
			var count int
			for i := 0; i < 10000; i++ {
				elm := uuid.NewV4().Bytes()
				check := b.Has([]byte(elm))
				if check {
					count++
				}
			}

			Convey("Then false positive incidence should be less than expected", func() {
				So(count, ShouldBeLessThan, 10000*fp)

			})
		})
	})
}

func TestScalableFilter_GobEncode(t *testing.T) {
	Convey("Given bloom filter", t, func() {
		m := 128
		gr := 2
		var fp, reduction float64
		fp = 0.0001
		reduction = 0.8

		sf := NewScalableFilter(m, gr, fp, reduction)

		e := []byte("test")
		sf.Add(e)

		Convey("When converting gobs stream", func() {
			buf, err := sf.GobEncode()

			Convey("Then expected bytes slice should be returned", func() {
				So(err, ShouldBeNil)
				So(len(buf), ShouldNotEqual, 0)

			})
		})
	})
}

func TestScalableFilter_GobDecode(t *testing.T) {
	Convey("Given bloom filter converted to gobs stream", t, func() {
		m := 128
		gr := 2
		var fp, reduction float64
		fp = 0.0001
		reduction = 0.8

		sf := NewScalableFilter(m, gr, fp, reduction)

		e := []byte("test")
		sf.Add(e)

		buf, _ := sf.GobEncode()

		Convey("When decoding gobs stream", func() {
			res := &ScalableFilter{}
			err := res.GobDecode(buf)

			Convey("Then expected bytes slice should be returned", func() {
				So(err, ShouldBeNil)
				So(len(res.filters), ShouldEqual, len(sf.filters))
				So(len(res.filters[0].bits), ShouldEqual, len(sf.filters[0].bits))
				So(res.m, ShouldEqual, sf.m)
				So(res.k, ShouldEqual, sf.k)
				So(res.n, ShouldEqual, sf.n)
				So(res.growthRate, ShouldEqual, sf.growthRate)
				So(res.fpReduction, ShouldEqual, sf.fpReduction)
				So(res.Has([]byte("test")), ShouldBeTrue)

			})
		})
	})
}

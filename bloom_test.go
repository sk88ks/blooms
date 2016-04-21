package blooms

import (
	"hash/fnv"
	"testing"

	"github.com/satori/go.uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNew(t *testing.T) {
	Convey("Given filter size, hasher number", t, func() {
		m := 128
		k := 5

		Convey("When creating a new bloom filter", func() {
			b := New(m, k, nil)

			Convey("Then created instance should be expected", func() {
				So(b, ShouldNotBeNil)
				So(len(b.bits), ShouldEqual, m)
				So(b.k, ShouldEqual, k)
				So(b.s, ShouldEqual, 0)

			})
		})
	})
}

func TestBaseFilter_Add(t *testing.T) {
	Convey("Given base filter", t, func() {
		m := 128
		k := 5

		b := &baseFilter{
			bits: make([]uint8, m),
			k:    k,
		}

		Convey("When adding a new element", func() {
			e := []byte("test")
			b.Add(e)

			Convey("Then element should be added", func() {
				So(b.n, ShouldEqual, 1)
				var count int
				for i := range b.bits {
					if b.bits[i] == 1 {
						count++
					}
				}
				So(count, ShouldEqual, k)

			})
		})
	})

	Convey("Given base filter with custom hasher", t, func() {
		m := 128
		k := 5
		hasher := fnv.New64()

		b := &baseFilter{
			bits:   make([]uint8, m),
			k:      k,
			hasher: hasher,
		}

		Convey("When adding a new element", func() {
			e := []byte("test")
			b.Add(e)

			Convey("Then element should be added", func() {
				So(b.n, ShouldEqual, 1)
				var count int
				for i := range b.bits {
					if b.bits[i] == 1 {
						count++
					}
				}
				So(count, ShouldEqual, k)

			})
		})
	})

	Convey("Given base filter with partition", t, func() {
		m := 128
		k := 5
		hasher := fnv.New64()
		s := int(m / k)

		b := &baseFilter{
			bits:   make([]uint8, m),
			k:      k,
			hasher: hasher,
			s:      s,
		}

		Convey("When adding a new element", func() {
			e := []byte("test")
			b.Add(e)

			Convey("Then element should be added", func() {
				So(b.n, ShouldEqual, 1)
				var count int
				previous := 0
				current := s
				for i := range b.bits {
					if b.bits[i] == 1 {
						So(i, ShouldBeBetweenOrEqual, previous, current)
						previous = current
						current += s
						count++
					}
				}
				So(count, ShouldEqual, k)

			})
		})
	})
}

func TestBaseFilter_Has(t *testing.T) {
	Convey("Given base filter and set element", t, func() {
		m := 128
		k := 5

		b := &baseFilter{
			bits: make([]uint8, m),
			k:    k,
		}

		e := []byte("test")
		b.Add(e)

		Convey("When check set element", func() {
			check := b.Has(e)

			Convey("Then true should be returned", func() {
				So(check, ShouldBeTrue)

			})
		})

		Convey("When check not set element", func() {
			check := b.Has([]byte("not_set"))

			Convey("Then false should be returned", func() {
				So(check, ShouldBeFalse)

			})
		})
	})

	Convey("Given base filter with custom hasher", t, func() {
		m := 128
		k := 5
		hasher := fnv.New64()

		b := &baseFilter{
			bits:   make([]uint8, m),
			k:      k,
			hasher: hasher,
		}

		e := []byte("test")
		b.Add(e)

		Convey("When check set element", func() {
			check := b.Has(e)

			Convey("Then true should be returned", func() {
				So(check, ShouldBeTrue)

			})
		})

		Convey("When check not set element", func() {
			check := b.Has([]byte("not_set"))

			Convey("Then false should be returned", func() {
				So(check, ShouldBeFalse)

			})
		})
	})

	Convey("Given base filter with partition", t, func() {
		m := 128
		k := 5
		hasher := fnv.New64()
		s := int(m / k)

		b := &baseFilter{
			bits:   make([]uint8, m),
			k:      k,
			hasher: hasher,
			s:      s,
		}

		e := []byte("test")
		b.Add(e)

		Convey("When check set element", func() {
			check := b.Has(e)

			Convey("Then true should be returned", func() {
				So(check, ShouldBeTrue)

			})
		})

		Convey("When check not set element", func() {
			check := b.Has([]byte("not_set"))

			Convey("Then false should be returned", func() {
				So(check, ShouldBeFalse)

			})
		})
	})
}

func TestBloomFilter_GetFalsePositiveIncidence(t *testing.T) {
	Convey("Given bloom filter and set element", t, func() {
		m := 128
		k := 2

		b := New(m, k, nil)

		for i := 0; i < 200; i++ {
			elm := uuid.NewV4().Bytes()
			b.Add(elm)
		}

		Convey("When getting false positive incidence", func() {
			fp := b.GetFalsePositiveIncidence()

			Convey("Then true should be returned", func() {
				So(fp, ShouldBeBetween, 0.9, 1)

			})
		})
	})

}

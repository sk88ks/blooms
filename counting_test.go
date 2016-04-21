package blooms

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewCountingFilter(t *testing.T) {
	Convey("Given filter size, hasher number", t, func() {
		m := 128
		k := 5

		Convey("When creating a new bloom filter", func() {
			b := NewCountingFilter(m, k, nil)

			Convey("Then created instance should be expected", func() {
				So(b, ShouldNotBeNil)
				So(len(b.bits), ShouldEqual, m)
				So(b.k, ShouldEqual, k)
				So(b.s, ShouldEqual, 0)

			})
		})
	})
}

func TestCountingFilter_Add(t *testing.T) {
	Convey("Given counting filter", t, func() {
		m := 128
		k := 5

		b := NewCountingFilter(m, k, nil)

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
				So(b.Has(e), ShouldBeTrue)

			})
		})
	})
}

func TestCountingFilter_Remove(t *testing.T) {
	Convey("Given counting filter", t, func() {
		m := 128
		k := 5

		b := NewCountingFilter(m, k, nil)

		e := []byte("test")
		b.Add(e)

		Convey("When removing a element", func() {
			b.Remove(e)

			Convey("Then element should remain", func() {
				So(b.n, ShouldEqual, 0)
				var count int
				for i := range b.bits {
					if b.bits[i] == 1 {
						count++
					}
				}
				So(count, ShouldEqual, 0)
				So(b.Has(e), ShouldBeFalse)

			})
		})

		Convey("When add twice and removing a element", func() {
			b.Add(e)
			b.Add(e)
			b.Remove(e)

			Convey("Then element should be removed", func() {
				So(b.n, ShouldEqual, 2)
				var count int
				for i := range b.bits {
					if b.bits[i] != 0 {
						count++
					}
				}
				So(count, ShouldEqual, k)
				So(b.Has(e), ShouldBeTrue)

			})
		})
	})
}

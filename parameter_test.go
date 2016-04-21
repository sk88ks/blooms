package blooms

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetBestHasherNumber(t *testing.T) {
	Convey("Given false positive incidence", t, func() {
		var p float64
		p = 0.25

		Convey("When getting minimum hasher number", func() {
			n := GetMinimumHasherNumber(p)

			Convey("Then expected number should be computed", func() {
				So(n, ShouldEqual, 2)

			})
		})
	})

}

func TestGetBestElementNumber(t *testing.T) {
	Convey("Given filter size and exepected false positive incidence", t, func() {
		m := 128
		var p float64
		p = 0.25

		Convey("When getting max element number", func() {
			n := GetBestElementNumber(m, p)

			Convey("Then expected number should be computed", func() {
				So(n, ShouldEqual, 44)

			})
		})
	})
}

func TestGetBestFilterSize(t *testing.T) {
	Convey("Given element number and exepected false positive incidence", t, func() {
		n := 44
		var p float64
		p = 0.25

		Convey("When getting appropriate filter size", func() {
			m := GetBestFilterSize(n, p)

			Convey("Then expected number should be computed", func() {
				So(m, ShouldEqual, 127)

			})
		})
	})
}

package blooms

import "math"

// GetMinimumHasherNumber compute the best hasher number (k)
// with expected false positive incidence
// as fill ratio (p) is 0.5
func GetMinimumHasherNumber(p float64) int {
	return int(math.Log2(1 / p))
}

// GetBestElementNumber compute the best element number (n)
// with filter size and expected false positive incidence
// as fill ratio (p) is 0.5
func GetBestElementNumber(m int, p float64) int {
	return int(float64(m) * math.Pow(math.Log(2), 2) / math.Abs(math.Log(p)))
}

//GetBestFilterSize compute the best filter size (m)
// with element number and expected false positive incidence
// as fill ratio (p) is 0.5
func GetBestFilterSize(n int, p float64) int {
	return int(float64(n)*math.Abs(math.Log(p))/math.Pow(math.Log(2), 2) + 1)
}

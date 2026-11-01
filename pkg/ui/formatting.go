package ui

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/SimonSchneider/goslu/date"
)

func FormatWithThousands(val float64) string {
	// Round the value to the nearest integer
	rounded := math.Round(val)
	s := strconv.FormatInt(int64(rounded), 10)
	n := len(s)
	neg := false
	if n > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
		n--
	}
	if n <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	var out []byte
	pre := n % 3
	if pre == 0 {
		pre = 3
	}
	out = append(out, s[:pre]...)
	for i := pre; i < n; i += 3 {
		out = append(out, ',')
		out = append(out, s[i:i+3]...)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// percentDecimals is how many decimals FormatPercent keeps: enough for any percentage typed, few
// enough to drop the float noise of scaling a fraction by 100.
const percentDecimals = 10

// FormatPercent formats a fraction as the percentage ParsePercent reads back: 0.15 is "15", and 0.07
// is "7" rather than "7.000000000000001".
func FormatPercent(fraction float64) string {
	s := strconv.FormatFloat(fraction*100, 'f', percentDecimals, 64)
	s = strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
	if s == "-0" {
		return "0"
	}
	return s
}

// FormatDays formats days with their sign, as ParseDays parses them: "+20", "-5" or "+0".
func FormatDays(d date.Duration) string {
	return fmt.Sprintf("%+d", d)
}

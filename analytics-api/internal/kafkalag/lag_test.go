package kafkalag

import "testing"

func TestCompute(t *testing.T) {
	cases := []struct {
		name                  string
		start, end, committed int64
		want                  int64
	}{
		{"caught up", 0, 100, 100, 0},
		{"behind", 0, 100, 60, 40},
		{"no commit yet: whole retained log", 10, 100, -1, 90},
		{"commit below retention start", 50, 100, 20, 50},
		{"never negative", 0, 100, 120, 0},
		{"empty partition", 0, 0, -1, 0},
	}
	for _, c := range cases {
		if got := Compute(c.start, c.end, c.committed); got != c.want {
			t.Errorf("%s: Compute(%d,%d,%d)=%d want %d", c.name, c.start, c.end, c.committed, got, c.want)
		}
	}
}

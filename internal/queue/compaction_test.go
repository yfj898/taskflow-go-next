package queue

import "testing"

func TestCompareStreamIDs(t *testing.T) {
	tests := []struct {
		left  string
		right string
		want  int
	}{
		{"1-0", "1-0", 0},
		{"1-1", "1-0", 1},
		{"2-0", "1-999", 1},
		{"9-4", "10-0", -1},
	}
	for _, tc := range tests {
		if got := compareStreamIDs(tc.left, tc.right); got != tc.want {
			t.Fatalf("compareStreamIDs(%q,%q)=%d, want %d", tc.left, tc.right, got, tc.want)
		}
	}
}

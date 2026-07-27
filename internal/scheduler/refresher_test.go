package scheduler

import "testing"

func TestSameStringSlice(t *testing.T) {
	tests := []struct {
		name  string
		left  []string
		right []string
		want  bool
	}{
		{name: "same", left: []string{"a", "b"}, right: []string{"a", "b"}, want: true},
		{name: "different order", left: []string{"a", "b"}, right: []string{"b", "a"}, want: false},
		{name: "different length", left: []string{"a"}, right: []string{"a", "b"}, want: false},
		{name: "both nil", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameStringSlice(tt.left, tt.right); got != tt.want {
				t.Fatalf("sameStringSlice(%v, %v) = %t, want %t", tt.left, tt.right, got, tt.want)
			}
		})
	}
}

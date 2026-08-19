package handlers

import (
	"testing"
	"time"
)

// TestTimeAgo_Boundaries pins the boundary transitions of the
// timeAgo template func: 1s/60s, 60s/60min, 60min/24h, 24h/7d.
// Each table case exercises a representative duration inside one
// of the four output bands (s/m/h/d) plus the exact transition
// points, so a regression in any of the four switch arms would
// fail one of these cases.
//
// "now" is captured per test so the input to timeAgo is a stable
// instant; the function itself uses time.Since internally, so
// there is up to a few microseconds of clock drift between the
// captured "now" and the read inside timeAgo. Durations are
// chosen with enough headroom on either side of each boundary
// that this drift can never change the output band.
func TestTimeAgo_Boundaries(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		dur  time.Duration
		want string
	}{
		{"1s", 1 * time.Second, "1s ago"},
		{"59s (under minute boundary)", 59 * time.Second, "59s ago"},
		{"60s (minute boundary)", 60 * time.Second, "1m ago"},
		{"119s (just under 2m)", 119 * time.Second, "1m ago"},
		{"2m", 2 * time.Minute, "2m ago"},
		{"59m (under hour boundary)", 59 * time.Minute, "59m ago"},
		{"60m (hour boundary)", 60 * time.Minute, "1h ago"},
		{"23h (under day boundary)", 23 * time.Hour, "23h ago"},
		{"24h (day boundary)", 24 * time.Hour, "1d ago"},
		{"7d", 7 * 24 * time.Hour, "7d ago"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := timeAgo(now.Add(-tc.dur))
			if got != tc.want {
				t.Errorf("timeAgo(now-%v): got %q want %q", tc.dur, got, tc.want)
			}
		})
	}
}

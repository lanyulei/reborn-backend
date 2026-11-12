package service

import (
	"errors"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListTimeRange(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(d)) }
	cases := []struct {
		name       string
		start, end *timestamppb.Timestamp
		wantStart  time.Time
		wantEndNil bool
		wantErr    bool
	}{
		{"both absent: last 7d, open end", nil, nil, now.Add(-defaultListWindow), true, false},
		{"end only: 7d before end", nil, ts(-time.Hour), now.Add(-time.Hour - defaultListWindow), false, false},
		{"start only within cap", ts(-30 * 24 * time.Hour), nil, now.Add(-30 * 24 * time.Hour), true, false},
		{"explicit both", ts(-48 * time.Hour), ts(-time.Hour), now.Add(-48 * time.Hour), false, false},
		{"span over cap", ts(-91 * 24 * time.Hour), nil, time.Time{}, true, true},
		{"explicit span over cap", ts(-200 * 24 * time.Hour), ts(-100 * 24 * time.Hour), time.Time{}, false, true},
	}
	for _, c := range cases {
		start, end, err := listTimeRange(c.start, c.end, now)
		if c.wantErr {
			if !errors.Is(err, biz.ErrTimeRangeInvalid) {
				t.Errorf("%s: err = %v, want ErrTimeRangeInvalid", c.name, err)
			}
			continue
		}
		if err != nil || start == nil || !start.Equal(c.wantStart) || (end == nil) != c.wantEndNil {
			t.Errorf("%s: = (%v, %v, %v), want start %v endNil=%v", c.name, start, end, err, c.wantStart, c.wantEndNil)
		}
	}
}

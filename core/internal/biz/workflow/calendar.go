package biz

import (
	"fmt"
	"time"
)

// maxCalendarDays 工作日历的节假日 / 调休上班日条数上限。
const maxCalendarDays = 400

// CalendarSegment 一段每日工作时段（HH:MM，左闭右开）。
type CalendarSegment struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// CalendarConfig 工作日历配置（jsonb 持久化，json 标签即契约）。
type CalendarConfig struct {
	// Timezone IANA 时区名：工作时段与节假日按它解释。
	Timezone string `json:"timezone"`
	// Weekdays 每周的工作日：0 = 周日 … 6 = 周六。
	Weekdays []int32 `json:"weekdays"`
	// Segments 每日工作时段，按时间先后、互不重叠。
	Segments []CalendarSegment `json:"segments"`
	// Holidays 节假日（yyyy-mm-dd）：即使是工作日也不上班。
	Holidays []string `json:"holidays"`
	// Workdays 调休上班日（yyyy-mm-dd）：即使是周末也上班。
	Workdays []string `json:"workdays"`
}

// Calendar 可用于推算时限的工作日历（由 CalendarConfig 编译而来，只读、可并发使用）。
type Calendar struct {
	loc      *time.Location
	weekdays [7]bool
	segments [][2]int // 自零点起的分钟数 [start, end)
	holidays map[string]struct{}
	workdays map[string]struct{}
	perDay   int // 每个工作日的工作分钟数
}

// parseClock 解析 HH:MM 为自零点起的分钟数；24:00 表示当日结束。
func parseClock(s string) (int, bool) {
	if s == "24:00" {
		return 24 * 60, true
	}
	t, err := time.Parse("15:04", s)
	if err != nil {
		return 0, false
	}
	return t.Hour()*60 + t.Minute(), true
}

// NewCalendar 校验并编译工作日历配置。
func NewCalendar(c CalendarConfig) (*Calendar, error) {
	loc, err := time.LoadLocation(c.Timezone)
	if err != nil || c.Timezone == "" {
		return nil, fmt.Errorf("unknown timezone %q", c.Timezone)
	}
	cal := &Calendar{loc: loc, holidays: map[string]struct{}{}, workdays: map[string]struct{}{}}
	if len(c.Weekdays) == 0 || len(c.Weekdays) > 7 {
		return nil, fmt.Errorf("at least one working weekday is required")
	}
	for _, d := range c.Weekdays {
		if d < 0 || d > 6 || cal.weekdays[d] {
			return nil, fmt.Errorf("invalid or duplicate weekday %d", d)
		}
		cal.weekdays[d] = true
	}
	if len(c.Segments) == 0 || len(c.Segments) > 6 {
		return nil, fmt.Errorf("1-6 working time segments are required")
	}
	prev := 0
	for _, seg := range c.Segments {
		start, ok1 := parseClock(seg.Start)
		end, ok2 := parseClock(seg.End)
		if !ok1 || !ok2 || start >= end || start < prev {
			return nil, fmt.Errorf("working time segments must be HH:MM ranges in order without overlap")
		}
		cal.segments = append(cal.segments, [2]int{start, end})
		cal.perDay += end - start
		prev = end
	}
	if len(c.Holidays) > maxCalendarDays || len(c.Workdays) > maxCalendarDays {
		return nil, fmt.Errorf("at most %d holidays and %d extra working days", maxCalendarDays, maxCalendarDays)
	}
	for _, set := range []struct {
		days []string
		into map[string]struct{}
	}{{c.Holidays, cal.holidays}, {c.Workdays, cal.workdays}} {
		for _, d := range set.days {
			if _, err := time.Parse(time.DateOnly, d); err != nil {
				return nil, fmt.Errorf("invalid date %q, want yyyy-mm-dd", d)
			}
			set.into[d] = struct{}{}
		}
	}
	return cal, nil
}

// working 报告某天（按日历时区的日期）是否上班：调休上班日 > 节假日 > 每周工作日。
func (c *Calendar) working(day time.Time) bool {
	key := day.Format(time.DateOnly)
	if _, ok := c.workdays[key]; ok {
		return true
	}
	if _, ok := c.holidays[key]; ok {
		return false
	}
	return c.weekdays[day.Weekday()]
}

// maxCalendarScan 推算时向后最多扫描的天数（配置成几乎不上班时的兜底，避免死循环）。
const maxCalendarScan = 3 * 366

// Add 自 from 起经过 d 之后的时刻。d.WorkTime 为 false 时按自然时间；为 true 时：
// 分钟 / 小时只消耗工作时段内的时间，天 = 工作日（落在 N 个工作日之后的同一钟点）。
func (c *Calendar) Add(from time.Time, d Duration) time.Time {
	if !d.WorkTime || c == nil {
		return from.Add(time.Duration(d.minutes()) * time.Minute)
	}
	local := from.In(c.loc)
	if d.Unit == UnitDay {
		day, left := local, d.Value
		for scanned := 0; left > 0 && scanned < maxCalendarScan; scanned++ {
			day = day.AddDate(0, 0, 1)
			if c.working(day) {
				left--
			}
		}
		return day.UTC()
	}
	// 逐个工作时段按真实时刻推算：时段的起止用日历时区的钟点构造成时刻，消耗的是时段内
	// 实际流逝的时间。夏令时切换日一天不是 24 小时、同一个钟点可能出现两次或不存在——用
	// 「零点 + 分钟数」或直接拼钟点都会算出偏一小时、甚至早于 from 的结果。
	left := time.Duration(d.minutes()) * time.Minute
	start := from.Truncate(time.Minute) // 秒向下取整
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, c.loc)
	for scanned := 0; scanned < maxCalendarScan; scanned++ {
		if c.working(day) {
			for _, seg := range c.segments {
				begin := time.Date(day.Year(), day.Month(), day.Day(), 0, seg[0], 0, 0, c.loc)
				end := time.Date(day.Year(), day.Month(), day.Day(), 0, seg[1], 0, 0, c.loc)
				if begin.Before(start) {
					begin = start
				}
				if !begin.Before(end) {
					continue
				}
				if span := end.Sub(begin); left <= span {
					return begin.Add(left).UTC()
				} else {
					left -= span
				}
			}
		}
		day = time.Date(day.Year(), day.Month(), day.Day()+1, 0, 0, 0, 0, c.loc)
	}
	return from.Add(time.Duration(d.minutes()) * time.Minute) // 日历几乎不上班：退回自然时间
}

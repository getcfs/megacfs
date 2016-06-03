package brimtime

import (
	"testing"
	"time"
)

func TestStartOfWeek(t *testing.T) {
	for in, exp := range map[time.Time]time.Time{
		time.Date(2014, 6, 29, 1, 2, 3, 4, time.UTC): time.Date(2014, 6, 29, 1, 2, 3, 4, time.UTC),
		time.Date(2014, 7, 2, 1, 2, 3, 4, time.UTC):  time.Date(2014, 6, 29, 1, 2, 3, 4, time.UTC),
		time.Date(2014, 7, 5, 1, 2, 3, 4, time.UTC):  time.Date(2014, 6, 29, 1, 2, 3, 4, time.UTC),
	} {
		out := StartOfWeek(in)
		if out != exp {
			t.Errorf("StartOfWeek(%s) %s != %s", in, out, exp)
		}
	}
}

func TestStartOfMonth(t *testing.T) {
	for in, exp := range map[time.Time]time.Time{
		time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC):  time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC),
		time.Date(2014, 7, 15, 1, 2, 3, 4, time.UTC): time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC),
		time.Date(2014, 7, 31, 1, 2, 3, 4, time.UTC): time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC),
		time.Date(2014, 7, 32, 1, 2, 3, 4, time.UTC): time.Date(2014, 8, 1, 1, 2, 3, 4, time.UTC),
	} {
		out := StartOfMonth(in)
		if out != exp {
			t.Errorf("StartOfMonth(%s) %s != %s", in, out, exp)
		}
	}
}

func TestSameDay(t *testing.T) {
	t1 := time.Date(2014, 7, 15, 1, 2, 3, 4, time.UTC)
	t2 := time.Date(2014, 7, 15, 1, 2, 3, 4, time.UTC)
	out := SameDay(t1, t2)
	exp := true
	if out != exp {
		t.Errorf("SameDay(%s, %s) %t != %t", t1, t2, out, exp)
	}
	t1 = time.Date(2014, 7, 15, 1, 2, 3, 4, time.UTC)
	t2 = time.Date(2014, 7, 16, 1, 2, 3, 4, time.UTC)
	out = SameDay(t1, t2)
	exp = false
	if out != exp {
		t.Errorf("SameDay(%s, %s) %t != %t", t1, t2, out, exp)
	}
	t1 = time.Date(2014, 7, 15, 0, 0, 0, 0, time.UTC)
	t2 = time.Date(2014, 7, 15, 23, 59, 59, 999999999, time.UTC)
	out = SameDay(t1, t2)
	exp = true
	if out != exp {
		t.Errorf("SameDay(%s, %s) %t != %t", t1, t2, out, exp)
	}
	t1 = time.Date(2014, 7, 15, 0, 0, 0, 0, time.UTC)
	t2 = time.Date(2014, 7, 15, 23, 59, 59, 1000000000, time.UTC)
	out = SameDay(t1, t2)
	exp = false
	if out != exp {
		t.Errorf("SameDay(%s, %s) %t != %t", t1, t2, out, exp)
	}
}

func TestNearDateString(t *testing.T) {
	for exp, in := range map[string][]time.Time{
		"Tuesday the 1st":               []time.Time{time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC), time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC)},
		"Wednesday the 2nd":             []time.Time{time.Date(2014, 7, 2, 1, 2, 3, 4, time.UTC), time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC)},
		"Monday, June 30th":             []time.Time{time.Date(2014, 6, 30, 1, 2, 3, 4, time.UTC), time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC)},
		"Monday, the 1st of July, 2013": []time.Time{time.Date(2013, 7, 1, 1, 2, 3, 4, time.UTC), time.Date(2014, 7, 1, 1, 2, 3, 4, time.UTC)},
	} {
		out := NearDateString(in[0], in[1])
		if out != exp {
			t.Errorf("NearDateString(%s) %s != %s", in, out, exp)
		}
	}
}

func TestTranslateMonth(t *testing.T) {
	for in, exp := range map[string]int{
		"jan":     1,
		"garbage": 0,
		"2":       2,
		"13":      0,
		"0":       0,
	} {
		out := TranslateMonth(in)
		if out != exp {
			t.Errorf("TranslateMonth(%s) %d != %d", in, out, exp)
		}
	}
}

func TestTranslateWeekday(t *testing.T) {
	for in, exp := range map[string]int{
		"sun":     0,
		"mon":     1,
		"sat":     6,
		"garbage": -1,
		"2":       2,
		"7":       0,
		"0":       0,
	} {
		out := TranslateWeekday(in)
		if out != exp {
			t.Errorf("TranslateWeekday(%s) %d != %d", in, out, exp)
		}
	}
}

func TestTranslateYMD(t *testing.T) {
	type ts struct {
		v string
		b []string
		y int
		m int
		d int
	}
	for _, tt := range []ts{
		ts{"2014-1-2", []string{"", "D", "MD", "YMD"}, 2014, 1, 2},
		ts{"2014-1-Feb", []string{"", "D", "MD", "YMD"}, 2014, 2, 1},
		ts{"2014-1-2", []string{"", "D", "MD", "YDM"}, 2014, 2, 1},
		ts{"2014-Jan-2", []string{"", "D", "MD", "YDM"}, 2014, 1, 2},
		ts{"1-2", []string{"", "D", "MD", "YMD"}, 0, 1, 2},
		ts{"1-Feb", []string{"", "D", "MD", "YMD"}, 0, 2, 1},
		ts{"Jan-2", []string{"", "D", "DM", "YMD"}, 0, 1, 2},
		ts{"1-2", []string{"", "D", "DM", "YMD"}, 0, 2, 1},
		ts{"1-31", []string{"", "D", "DM", "YMD"}, 0, 1, 31},
		ts{"1-2-2014", []string{"", "D", "MD", "YMD"}, 2014, 1, 2},
		ts{"1-Feb-2014", []string{"", "D", "MD", "YMD"}, 2014, 2, 1},
		ts{"Feb-1-2014", []string{"", "D", "MD", "YMD"}, 2014, 2, 1},
		ts{"2014,1/2", []string{"", "D", "MD", "YMD"}, 2014, 1, 2},
		ts{"Jan-Feb-2014", []string{"", "D", "MD", "YMD"}, 0, 0, 0},
		ts{"garbage", []string{"", "D", "MD", "YMD"}, 0, 0, 0},
		ts{"", []string{"", "D", "MD", "YMD"}, 0, 0, 0},
	} {
		y, m, d := TranslateYMD(tt.v, tt.b)
		if y != tt.y || m != tt.m || d != tt.d {
			t.Errorf("TranslateYMD(%#v, %#v) %d, %d, %d != %d, %d, %d", tt.v, tt.b, y, m, d, tt.y, tt.m, tt.d)
		}
	}
}

func TestTranslateDateRef(t *testing.T) {
	type ts struct {
		v string
		b []string
		e time.Time
	}
	ref := time.Date(1901, 11, 22, 4, 5, 6, 7, time.UTC)
	for _, tt := range []ts{
		ts{"2014-1-2", []string{"", "D", "MD", "YMD"}, time.Date(2014, 1, 2, 4, 5, 6, 7, time.UTC)},
		ts{"2014-1-Feb", []string{"", "D", "MD", "YMD"}, time.Date(2014, 2, 1, 4, 5, 6, 7, time.UTC)},
		ts{"2014-1-2", []string{"", "D", "MD", "YDM"}, time.Date(2014, 2, 1, 4, 5, 6, 7, time.UTC)},
		ts{"2014-Jan-2", []string{"", "D", "MD", "YDM"}, time.Date(2014, 1, 2, 4, 5, 6, 7, time.UTC)},
		ts{"1-2", []string{"", "D", "MD", "YMD"}, time.Date(1901, 1, 2, 4, 5, 6, 7, time.UTC)},
		ts{"1-Feb", []string{"", "D", "MD", "YMD"}, time.Date(1901, 2, 1, 4, 5, 6, 7, time.UTC)},
		ts{"Jan-2", []string{"", "D", "DM", "YMD"}, time.Date(1901, 1, 2, 4, 5, 6, 7, time.UTC)},
		ts{"1-2", []string{"", "D", "DM", "YMD"}, time.Date(1901, 2, 1, 4, 5, 6, 7, time.UTC)},
		ts{"1-31", []string{"", "D", "DM", "YMD"}, time.Date(1901, 1, 31, 4, 5, 6, 7, time.UTC)},
		ts{"1-2-2014", []string{"", "D", "MD", "YMD"}, time.Date(2014, 1, 2, 4, 5, 6, 7, time.UTC)},
		ts{"1-Feb-2014", []string{"", "D", "MD", "YMD"}, time.Date(2014, 2, 1, 4, 5, 6, 7, time.UTC)},
		ts{"Feb-1-2014", []string{"", "D", "MD", "YMD"}, time.Date(2014, 2, 1, 4, 5, 6, 7, time.UTC)},
		ts{"2014,1/2", []string{"", "D", "MD", "YMD"}, time.Date(2014, 1, 2, 4, 5, 6, 7, time.UTC)},
		ts{"Jan-Feb-2014", []string{"", "D", "MD", "YMD"}, time.Date(1901, 11, 22, 4, 5, 6, 7, time.UTC)},
		ts{"garbage", []string{"", "D", "MD", "YMD"}, time.Date(1901, 11, 22, 4, 5, 6, 7, time.UTC)},
		ts{"", []string{"", "D", "MD", "YMD"}, time.Date(1901, 11, 22, 4, 5, 6, 7, time.UTC)},
	} {
		o := TranslateDateRef(tt.v, tt.b, ref)
		if o != tt.e {
			t.Errorf("TranslateDateRef(%#v, %#v, %s) %s != %s", tt.v, tt.b, ref, o, tt.e)
		}
	}
}

func TestTranslateRelativeDate(t *testing.T) {
	ref := time.Date(1901, 11, 22, 4, 5, 6, 7, time.UTC)
	for v, exp := range map[string]time.Time{
		"1 day ago":           time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"1 days ago":          time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"-1 days from now":    time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"-1 days":             time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"1 days":              time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"-1 day":              time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"1 day":               time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"-1d":                 time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"1d":                  time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"1 month ago":         time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"1 months ago":        time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"-1 months":           time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"1 months":            time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"-1 month":            time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"1 month":             time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"-1m":                 time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"1m":                  time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"1 week ago":          time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"1 weeks ago":         time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"-1 weeks":            time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"1 weeks":             time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"-1 week":             time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"1 week":              time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"-1w":                 time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"1w":                  time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"1 year ago":          time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"1 years ago":         time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"-1 years":            time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"1 years":             time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"-1 year":             time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"1 year":              time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"-1y":                 time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"1y":                  time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"7 days from now":     time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"a day ago":           time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"a day from now":      time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"a day":               time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"a month ago":         time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"a month from now":    time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"a month":             time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"a week ago":          time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"a week from now":     time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"a week":              time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"a year ago":          time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"a year from now":     time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"a year":              time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"complete garbage":    time.Time{},
		"garbage years ago":   time.Time{},
		"in a day from now":   time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"in a day":            time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"in a month from now": time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"in a month":          time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"in a week from now":  time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"in a week":           time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"in a year from now":  time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"in a year":           time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"last day":            time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"last month":          time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"last week":           time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"last year":           time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"next day":            time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"next month":          time.Date(1901, 12, 22, 4, 5, 6, 7, time.UTC),
		"next week":           time.Date(1901, 11, 29, 4, 5, 6, 7, time.UTC),
		"next year":           time.Date(1902, 11, 22, 4, 5, 6, 7, time.UTC),
		"previous day":        time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
		"previous month":      time.Date(1901, 10, 22, 4, 5, 6, 7, time.UTC),
		"previous week":       time.Date(1901, 11, 15, 4, 5, 6, 7, time.UTC),
		"previous year":       time.Date(1900, 11, 22, 4, 5, 6, 7, time.UTC),
		"tommorow":            time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"tommorrow":           time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"tomorow":             time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"tomorrow":            time.Date(1901, 11, 23, 4, 5, 6, 7, time.UTC),
		"yesterday":           time.Date(1901, 11, 21, 4, 5, 6, 7, time.UTC),
	} {
		out := TranslateRelativeDate(v, ref)
		if out != exp {
			t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
		}
	}
	v := "1 month ago"
	ref = time.Date(2015, 3, 30, 4, 5, 6, 7, time.UTC)
	out := TranslateRelativeDate(v, ref)
	exp := time.Date(2015, 2, 28, 4, 5, 6, 7, time.UTC)
	if out != exp {
		t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
	}
	v = "next month"
	ref = time.Date(2015, 1, 30, 4, 5, 6, 7, time.UTC)
	out = TranslateRelativeDate(v, ref)
	exp = time.Date(2015, 2, 28, 4, 5, 6, 7, time.UTC)
	if out != exp {
		t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
	}
	v = "1 month ago"
	ref = time.Date(2004, 3, 30, 4, 5, 6, 7, time.UTC)
	out = TranslateRelativeDate(v, ref)
	exp = time.Date(2004, 2, 29, 4, 5, 6, 7, time.UTC)
	if out != exp {
		t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
	}
	v = "next month"
	ref = time.Date(2004, 1, 30, 4, 5, 6, 7, time.UTC)
	out = TranslateRelativeDate(v, ref)
	exp = time.Date(2004, 2, 29, 4, 5, 6, 7, time.UTC)
	if out != exp {
		t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
	}
	v = "1 year ago"
	ref = time.Date(2004, 2, 29, 4, 5, 6, 7, time.UTC)
	out = TranslateRelativeDate(v, ref)
	exp = time.Date(2003, 2, 28, 4, 5, 6, 7, time.UTC)
	if out != exp {
		t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
	}
	v = "next year"
	ref = time.Date(2004, 2, 29, 4, 5, 6, 7, time.UTC)
	out = TranslateRelativeDate(v, ref)
	exp = time.Date(2005, 2, 28, 4, 5, 6, 7, time.UTC)
	if out != exp {
		t.Errorf("TranslateRelativeDate(%#v, %s) %s != %s", v, ref, out, exp)
	}
}

func TestAtForString(t *testing.T) {
	for exp, in := range map[string][]int{
		"@12:00":             []int{12, 0, 0},
		"@12:30":             []int{12, 30, 0},
		"@13:30":             []int{13, 30, 0},
		"@13:30 for 30m":     []int{13, 30, 30},
		"@13:30 for 1h":      []int{13, 30, 60},
		"@13:30 for 1h30m":   []int{13, 30, 90},
		"@13:30 for 1d":      []int{13, 30, 1440},
		"@13:30 for 1d1h":    []int{13, 30, 1500},
		"@13:30 for 1d1h30m": []int{13, 30, 1530},
		"@13:30 for 1d30m":   []int{13, 30, 1470},
		"@13:00 for 1d30m":   []int{13, 80, 1470},
		"":                   []int{80, 0, 0},
		"@00:00 for 30m":     []int{80, 0, 30},
		"for 1d":             []int{0, 0, 1440},
		"@00:00 for 1d1h":    []int{0, 0, 1500},
		"for 2d":             []int{0, 0, 2880},
	} {
		out := AtForString(in[0], in[1], in[2])
		if out != exp {
			t.Errorf("AtForString(%d, %d, %d) %#v != %#v", in[0], in[1], in[2], out, exp)
		}
	}
	in := []int{0, 0, 0}
	exp := ""
	out := AtForString(in[0], in[1], in[2])
	if out != exp {
		t.Errorf("AtForString(%d, %d, %d) %#v != %#v", in[0], in[1], in[2], out, exp)
	}
}

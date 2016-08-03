// Package brimtime contains tools for working with dates and times.
package brimtime

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gholt/brimtext"
)

var loweredMonths []string
var loweredWeekdays []string

func init() {
	loweredMonths = append(loweredMonths, "")
	for m := 1; m <= 12; m++ {
		loweredMonths = append(loweredMonths, strings.ToLower(time.Month(m).String()))
	}
	for w := 0; w <= 6; w++ {
		loweredWeekdays = append(loweredWeekdays, strings.ToLower(time.Weekday(w).String()))
	}
}

// StartOfWeek returns a time for the beginning of the week, Sunday, at the
// same time as the reference.
func StartOfWeek(reference time.Time) time.Time {
	return time.Date(reference.Year(), reference.Month(), reference.Day()-int(reference.Weekday()), reference.Hour(), reference.Minute(), reference.Second(), reference.Nanosecond(), reference.Location())
}

// StartOfMonth returns a time for the beginning of the month, the 1st, at the
// same time as the reference.
func StartOfMonth(reference time.Time) time.Time {
	return time.Date(reference.Year(), reference.Month(), 1, reference.Hour(), reference.Minute(), reference.Second(), reference.Nanosecond(), reference.Location())
}

// SameDay returns true if the two times are on the same day.
func SameDay(left time.Time, right time.Time) bool {
	return left.Year() == right.Year() && left.Month() == right.Month() && left.Day() == right.Day()
}

// NearDateString returns a string that represents the date, with less
// resolution the nearer it is to now (e.g. "Thursday, the 4th of July, 2013"
// "Thursday the 4th"). If now.IsZero() then time.Now() will be used.
func NearDateString(reference time.Time, now time.Time) string {
	if now.IsZero() {
		now = time.Now()
	}
	if reference.Year() != now.Year() {
		return fmt.Sprintf("%s, the %d%s of %s, %d", reference.Weekday(), reference.Day(), brimtext.OrdinalSuffix(reference.Day()), reference.Month(), reference.Year())
	} else if reference.Month() != now.Month() {
		return fmt.Sprintf("%s, %s %d%s", reference.Weekday(), reference.Month(), reference.Day(), brimtext.OrdinalSuffix(reference.Day()))
	}
	return fmt.Sprintf("%s the %d%s", reference.Weekday(), reference.Day(), brimtext.OrdinalSuffix(reference.Day()))
}

// TranslateMonth returns the month the value represents, or 0 if it cannot be
// determined. The value may be a month name, prefix, number (e.g. "August"
// "Aug" "8" "08") and is not case sensitive.
func TranslateMonth(value string) int {
	value = strings.ToLower(value)
	for m := 1; m <= 12; m++ {
		if strings.HasPrefix(loweredMonths[m], value) {
			return m
		}
	}
	m, err := strconv.Atoi(value)
	if err != nil || m < 1 || m > 12 {
		return 0
	}
	return m
}

// TranslateWeekday returns the weekday the value represents, or -1 if it
// cannot be determined. The value may be a weekday name, prefix, or number
// (e.g. "Monday" "Mon" "1" "01") and is not case sensitive.
func TranslateWeekday(value string) int {
	value = strings.ToLower(value)
	for w := 0; w <= 6; w++ {
		if strings.HasPrefix(loweredWeekdays[w], value) {
			return w
		}
	}
	w := 0
	if n, err := fmt.Sscanf(value, "%d", &w); err != nil || n != 1 {
		return -1
	}
	return w % 7
}

// TranslateYMD returns the year, month, and day the value represents, or zeros
// for the parts that cannot be determined. The biases field indicates the
// priority to use when guesses must be made. For example, if the value is
// "1-2-3", a bias of "YMD" would return 1, 2, 3 but a bias of "MDY" would
// return 3, 1, 2. Each bias in the list of biases is for that number of items
// given in the value. For example, "1-2-3" would use biases[3] as the bias,
// "1-2" would use biases[2], etc.; therefore the biases list should be four
// elements long.
//
// The value can be of various formats, but boils down to 1-3 items separated
// by one of the following characters "-/., ". Each item can represent a year,
// month, or day and the months can be numbers, names, or prefixes. Some
// examples: "1-2-2014" "2-Jan-2014" "January 2, 2014" "2,jan"
func TranslateYMD(value string, biases []string) (int, int, int) {
	parts := []string{}
	for {
		if i := strings.IndexAny(value, "-/., "); i == -1 {
			if len(value) > 0 {
				parts = append(parts, value)
			}
			break
		} else {
			if i > 0 {
				parts = append(parts, value[:i])
			}
			value = value[i+1:]
		}
	}
	if len(parts) == 0 || len(parts) > 3 {
		return 0, 0, 0
	}
	bias := strings.ToLower(biases[len(parts)])
	bias_year := strings.Index(bias, "y")
	if bias_year == -1 {
		bias_year = 3
	}
	bias_month := strings.Index(bias, "m")
	if bias_month == -1 {
		bias_month = 3
	}
	bias_day := strings.Index(bias, "d")
	if bias_day == -1 {
		bias_day = 3
	}
	ipart0, ipart1, ipart2 := 0, 0, 0
	spart0, spart1, spart2 := "", "", ""
	stringed := 0
	if len(parts) > 2 {
		n, err := fmt.Sscanf(parts[2], "%d", &ipart2)
		if err != nil || n != 1 {
			spart2 = parts[2]
			stringed++
		}
	}
	if len(parts) > 1 {
		n, err := fmt.Sscanf(parts[1], "%d", &ipart1)
		if err != nil || n != 1 {
			spart1 = parts[1]
			stringed++
		}
	}
	if len(parts) > 0 {
		n, err := fmt.Sscanf(parts[0], "%d", &ipart0)
		if err != nil || n != 1 {
			spart0 = parts[0]
			stringed++
		}
	}
	if stringed > 1 {
		return 0, 0, 0
	}
	year, month, day := 0, 0, 0
	if spart0 != "" {
		month, year, day = TranslateMonth(spart0), ipart1, ipart2
		if bias_day < bias_year {
			day, year = year, day
		}
		if day > 31 {
			day, year = year, day
		}
	} else if spart1 != "" {
		year, month, day = ipart0, TranslateMonth(spart1), ipart2
		if bias_day < bias_year {
			day, year = year, day
		}
		if day > 31 {
			day, year = year, day
		}
	} else if spart2 != "" {
		year, day, month = ipart0, ipart1, TranslateMonth(spart2)
		if bias_day < bias_year {
			day, year = year, day
		}
		if day > 31 {
			day, year = year, day
		}
	} else if ipart0 > 31 {
		year, month, day = ipart0, ipart1, ipart2
		if bias_day < bias_month {
			day, month = month, day
		}
		if month > 12 {
			day, month = month, day
		}
	} else if ipart1 > 31 {
		month, year, day = ipart0, ipart1, ipart2
		if bias_day < bias_month {
			day, month = month, day
		}
		if month > 12 {
			day, month = month, day
		}
	} else if ipart2 > 31 {
		month, day, year = ipart0, ipart1, ipart2
		if bias_day < bias_month {
			day, month = month, day
		}
		if month > 12 {
			day, month = month, day
		}
	} else if bias_year < bias_month {
		year, month, day = ipart0, ipart1, ipart2
		if bias_day < bias_month {
			day, month = month, day
		}
		if month > 12 {
			day, month = month, day
		}
	} else if bias_year < bias_day {
		month, year, day = ipart0, ipart1, ipart2
		if month > 12 {
			day, month = month, day
		}
	} else if bias_month < bias_day {
		month, day, year = ipart0, ipart1, ipart2
		if month > 12 {
			day, month = month, day
		}
	} else {
		day, month, year = ipart0, ipart1, ipart2
		if month > 12 {
			month, day = day, month
		}
	}
	return year, month, day
}

// TranslateDateRef returns the time the value represents with undetermined
// items filled in from the reference time. The biases list is for the call to
// TranslateYMD that is made, see there for more detail.
func TranslateDateRef(value string, biases []string, reference time.Time) time.Time {
	year, month, day := TranslateYMD(value, biases)
	if year == 0 {
		year = reference.Year()
	}
	if month == 0 {
		month = int(reference.Month())
	}
	if day == 0 {
		day = reference.Day()
	}
	return time.Date(year, time.Month(month), day, reference.Hour(), reference.Minute(), reference.Second(), reference.Nanosecond(), reference.Location())
}

// TranslateRelativeDate returns the time the value represents with
// undetermined items filled in from the reference time. The value may be
// various relative expressions, such as: "tomorrow", "last week", "3 weeks
// from now", "five days ago", etc. If the value cannot be parsed correctly,
// time.IsZero() will be returned.
func TranslateRelativeDate(value string, reference time.Time) time.Time {
	value = strings.ToLower(strings.Trim(value, " \r\n"))
	if strings.HasPrefix(value, "in ") {
		value = strings.Trim(value[3:], " \r\n")
	}
	years, months, days := 0, 0, 0
	if value == "tomorrow" || value == "tomorow" || value == "tommorow" || value == "tommorrow" || value == "next day" || value == "a day" || value == "a day from now" {
		days = 1
	} else if value == "next week" || value == "a week" || value == "a week from now" {
		days = 7
	} else if value == "next month" || value == "a month" || value == "a month from now" {
		months = 1
	} else if value == "next year" || value == "a year" || value == "a year from now" {
		years = 1
	} else if value == "yesterday" || value == "last day" || value == "previous day" || value == "a day ago" {
		days = -1
	} else if value == "last week" || value == "previous week" || value == "a week ago" {
		days = -7
	} else if value == "last month" || value == "previous month" || value == "a month ago" {
		months = -1
	} else if value == "last year" || value == "previous year" || value == "a year ago" {
		years = -1
	} else if strings.HasSuffix(value, " day from now") || strings.HasSuffix(value, " days from now") || strings.HasSuffix(value, " day") || strings.HasSuffix(value, " days") || strings.HasSuffix(value, "d") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &days)
		if err != nil || n != 1 {
			return time.Time{}
		}
	} else if strings.HasSuffix(value, " week from now") || strings.HasSuffix(value, " weeks from now") || strings.HasSuffix(value, " week") || strings.HasSuffix(value, " weeks") || strings.HasSuffix(value, "w") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &days)
		if err != nil || n != 1 {
			return time.Time{}
		}
		days *= 7
	} else if strings.HasSuffix(value, " month from now") || strings.HasSuffix(value, " months from now") || strings.HasSuffix(value, " month") || strings.HasSuffix(value, " months") || strings.HasSuffix(value, "m") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &months)
		if err != nil || n != 1 {
			return time.Time{}
		}
	} else if strings.HasSuffix(value, " year from now") || strings.HasSuffix(value, " years from now") || strings.HasSuffix(value, " year") || strings.HasSuffix(value, " years") || strings.HasSuffix(value, "y") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &years)
		if err != nil || n != 1 {
			return time.Time{}
		}
	} else if strings.HasSuffix(value, " day ago") || strings.HasSuffix(value, " days ago") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &days)
		if err != nil || n != 1 {
			return time.Time{}
		}
		days = -days
	} else if strings.HasSuffix(value, " week ago") || strings.HasSuffix(value, " weeks ago") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &days)
		if err != nil || n != 1 {
			return time.Time{}
		}
		days *= -7
	} else if strings.HasSuffix(value, " month ago") || strings.HasSuffix(value, " months ago") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &months)
		if err != nil || n != 1 {
			return time.Time{}
		}
		months = -months
	} else if strings.HasSuffix(value, " year ago") || strings.HasSuffix(value, " years ago") {
		n, err := fmt.Sscanf(strings.SplitN(value, " ", 1)[0], "%d", &years)
		if err != nil || n != 1 {
			return time.Time{}
		}
		years = -years
	} else {
		return time.Time{}
	}
	candidate := time.Date(reference.Year()+years, time.Month(int(reference.Month())+months), reference.Day()+days, reference.Hour(), reference.Minute(), reference.Second(), reference.Nanosecond(), reference.Location())
	if months != 0 || years != 0 {
		targetMonth := int(reference.Month()) + months
		for targetMonth < 1 {
			targetMonth += 12
		}
		targetMonth = (targetMonth-1)%12 + 1
		for int(candidate.Month()) != targetMonth {
			days -= 1
			candidate = time.Date(reference.Year()+years, time.Month(int(reference.Month())+months), reference.Day()+days, reference.Hour(), reference.Minute(), reference.Second(), reference.Nanosecond(), reference.Location())
		}
	}
	return candidate
}

// AtForString formats the hour, minute, and duration (in minutes) into a
// concise display string, such as: "@11:00" "@15:30 for 30m" "for 2h"
func AtForString(hour int, minute int, duration int) string {
	if hour < 0 || hour > 23 {
		if duration > 0 {
			hour = 0
		} else {
			return ""
		}
	}
	if hour == 0 && minute == 0 && duration == 0 {
		return ""
	}
	atString := fmt.Sprintf("@%02d", hour)
	if minute < 0 || minute > 59 {
		minute = 0
	}
	atString += fmt.Sprintf(":%02d", minute)
	if duration > 0 {
		var forString string
		days := duration / 1440
		hours := duration / 60 % 24
		minutes := duration % 60
		if days > 0 {
			forString += fmt.Sprintf("%dd", days)
		}
		if hours > 0 {
			forString += fmt.Sprintf("%dh", hours)
		}
		if minutes > 0 {
			forString += fmt.Sprintf("%dm", minutes)
		}
		if days > 0 && hours == 0 && minutes == 0 && hour == 0 && minute == 0 {
			return "for " + forString
		} else {
			return atString + " for " + forString
		}
	} else {
		return atString
	}
}

func TimeToUnixMicro(t time.Time) int64 {
	return t.Unix()*1000000 + int64(t.Nanosecond())/1000
}

func UnixMicroToTime(t int64) time.Time {
	return time.Unix(t/1000000, (t%1000000)*1000)
}

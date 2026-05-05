// Copyright (c) 2022-present PocketBase contributors.
// SPDX-License-Identifier: MIT
//
// Vendored from https://github.com/pocketbase/pocketbase/tree/master/tools/cron.

package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Moment represents a parsed single time moment.
type Moment struct {
	Minute    int `json:"minute"`
	Hour      int `json:"hour"`
	Day       int `json:"day"`
	Month     int `json:"month"`
	DayOfWeek int `json:"dayOfWeek"`
}

// NewMoment creates a new Moment from the specified time.
func NewMoment(t time.Time) Moment {
	return Moment{
		Minute:    t.Minute(),
		Hour:      t.Hour(),
		Day:       t.Day(),
		Month:     int(t.Month()),
		DayOfWeek: int(t.Weekday()),
	}
}

// Schedule stores parsed information for each time component.
type Schedule struct {
	Minutes    map[int]struct{} `json:"minutes"`
	Hours      map[int]struct{} `json:"hours"`
	Days       map[int]struct{} `json:"days"`
	Months     map[int]struct{} `json:"months"`
	DaysOfWeek map[int]struct{} `json:"daysOfWeek"`

	rawExpr string
}

// IsDue checks whether the provided Moment satisfies the current Schedule.
func (s *Schedule) IsDue(m Moment) bool {
	if _, ok := s.Minutes[m.Minute]; !ok {
		return false
	}
	if _, ok := s.Hours[m.Hour]; !ok {
		return false
	}
	if _, ok := s.Days[m.Day]; !ok {
		return false
	}
	if _, ok := s.DaysOfWeek[m.DayOfWeek]; !ok {
		return false
	}
	if _, ok := s.Months[m.Month]; !ok {
		return false
	}
	return true
}

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// NewSchedule creates a new Schedule from a cron expression.
//
// Supports 5-segment crontab expressions (minute hour dom month dow) plus the
// standard @yearly/@annually/@monthly/@weekly/@daily/@midnight/@hourly macros.
// Segment formats: wildcard (*), range (1-30), step (*/n or 1-30/n), and lists.
func NewSchedule(cronExpr string) (*Schedule, error) {
	if v, ok := macros[cronExpr]; ok {
		cronExpr = v
	}

	segments := strings.Fields(cronExpr)
	if len(segments) != 5 {
		return nil, errors.New("invalid cron expression - must be a valid macro or to have exactly 5 space separated segments")
	}

	minutes, err := parseCronSegment(segments[0], 0, 59)
	if err != nil {
		return nil, err
	}
	hours, err := parseCronSegment(segments[1], 0, 23)
	if err != nil {
		return nil, err
	}
	days, err := parseCronSegment(segments[2], 1, 31)
	if err != nil {
		return nil, err
	}
	months, err := parseCronSegment(segments[3], 1, 12)
	if err != nil {
		return nil, err
	}
	daysOfWeek, err := parseCronSegment(segments[4], 0, 6)
	if err != nil {
		return nil, err
	}

	return &Schedule{
		Minutes:    minutes,
		Hours:      hours,
		Days:       days,
		Months:     months,
		DaysOfWeek: daysOfWeek,
		rawExpr:    cronExpr,
	}, nil
}

func parseCronSegment(segment string, min int, max int) (map[int]struct{}, error) {
	slots := make(map[int]struct{}, max-min+1)
	for _, part := range strings.Split(segment, ",") {
		rangeMin, rangeMax, step, err := parseCronSegmentPart(part, min, max)
		if err != nil {
			return nil, err
		}
		for i := rangeMin; i <= rangeMax; i += step {
			slots[i] = struct{}{}
		}
	}
	return slots, nil
}

func parseCronSegmentPart(part string, min int, max int) (int, int, int, error) {
	base, step, err := parseCronStep(part, max)
	if err != nil {
		return 0, 0, 0, err
	}
	rangeMin, rangeMax, err := parseCronRange(base, step, min, max)
	return rangeMin, rangeMax, step, err
}

func parseCronStep(part string, max int) (string, int, error) {
	stepParts := strings.Split(part, "/")
	if len(stepParts) == 1 {
		return stepParts[0], 1, nil
	}
	if len(stepParts) != 2 {
		return "", 0, errors.New("invalid segment step format - must be in the format */n or 1-30/n")
	}
	step, err := strconv.Atoi(stepParts[1])
	if err != nil {
		return "", 0, err
	}
	if step < 1 || step > max {
		return "", 0, fmt.Errorf("invalid segment step boundary - the step must be between 1 and the %d", max)
	}
	return stepParts[0], step, nil
}

func parseCronRange(base string, step int, min int, max int) (int, int, error) {
	if base == "*" {
		return min, max, nil
	}
	rangeParts := strings.Split(base, "-")
	if len(rangeParts) == 1 {
		return parseCronSingleValue(rangeParts[0], step, min, max)
	}
	if len(rangeParts) == 2 {
		return parseCronRangeValues(rangeParts[0], rangeParts[1], min, max)
	}
	return 0, 0, errors.New("invalid segment range format - the range must have 1 or 2 parts")
}

func parseCronSingleValue(raw string, step int, min int, max int) (int, int, error) {
	if step != 1 {
		return 0, 0, errors.New("invalid segment step - step > 1 could be used only with the wildcard or range format")
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, 0, err
	}
	if parsed < min || parsed > max {
		return 0, 0, errors.New("invalid segment value - must be between the min and max of the segment")
	}
	return parsed, parsed, nil
}

func parseCronRangeValues(rawMin string, rawMax string, min int, max int) (int, int, error) {
	rangeMin, err := strconv.Atoi(rawMin)
	if err != nil {
		return 0, 0, err
	}
	if rangeMin < min || rangeMin > max {
		return 0, 0, fmt.Errorf("invalid segment range minimum - must be between %d and %d", min, max)
	}
	rangeMax, err := strconv.Atoi(rawMax)
	if err != nil {
		return 0, 0, err
	}
	if rangeMax < rangeMin || rangeMax > max {
		return 0, 0, fmt.Errorf("invalid segment range maximum - must be between %d and %d", rangeMin, max)
	}
	return rangeMin, rangeMax, nil
}

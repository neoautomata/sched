/*
Package sched provides additional schedule capabilities to
github.com/robconfig/cron:
  - Randomised offsets from another schedule
  - A schedule of Sunrises at a specific lat/long
  - A schedule of Sunsets at a specific lat/long
  - Subtract a specific offset from another schedule
  - Add a specific offset to another schedule
  - A schedule which is the union of two or more others

It also provides the ability to parse a JSON config into a Schedule.

The next methods are intended to be called with monotonically increasing times,
and may not work as intended otherwise.

RANDOM

Allows randomizing the time returned by another schedule. A max offset is
chosen, and the time from the wrapped shedule is added to or subtracted
by a random amount up to the maximum offset. Only second granularity is
supported.

  s := NewRandom(maxOffset, wrappedSchedule)
  t := s.Next(time.Now())

SUNRISE

A schedule of sunrises at a specific lat/long coordinate. North and West are
positive, while South and East are negative.

  s := NewSunrise(lat, long)
  s.Next(time.Now()) // the next sunrise

SUNSET

A schedule of sunsets at a specific lat/long coordinate. North and West are
positive, while South and East are negative.

  s := NewSunset(lat, long)
  s.Next(time.Now()) // the next sunset

SUBTRACT

Remove a specified offset from the wrapped schedule.

  s := NewSubtract(5 * time.Minute, wrappedSchedule)
  s.Next(time.Now())

ADD

Add a specified offset from the wrapped schedule.

  s := NewAdd(5 * time.Minute, wrappedSchedule)
  s.Next(time.Now())

SUPPRESS

Suppress instances of a wrapped schedule beginning at a specified time for a
specified duration. The suppressed period may be added or changed after
creation.

  s := NewEmptySuppress(wrappedSchedule)
  s.Suppress(start, duration)

UNION

Union combines two or more schedules and returns the next instance when all the
schedules report a time.

  u := NewUnsion(sched1, sched2, ...)
  s.Next(time.Now())

LICENSE

   Copyright 2017 neoautomata

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package sched

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cpucycle/astrotime"
	"github.com/robfig/cron"
)

// random contains the max offest and wrapped Schedule.
type random struct {
	offset time.Duration
	sched  cron.Schedule

	lastWrapped, lastOut time.Time
}

// NewRandom creates a randomizer with the specified maximum offset and
// schedule.
func NewRandom(maxOffset time.Duration, sched cron.Schedule) cron.Schedule {
	return &random{offset: maxOffset, sched: sched}
}

// Next retrieves a time from the wrapped Schedule and offsets it by up to +/-
// the maximum offset. It satisfies the Schedule interface.
func (r *random) Next(t time.Time) time.Time {
	nextWrapped := r.sched.Next(t)

	// Random can subtract time. If the last wrapped time is equal to the next
	// then time was subtracted. Return the last out (the substracted time) if
	// if it after t, otherwise move forward.
	if r.lastWrapped.Equal(nextWrapped) {
		if r.lastOut.After(t) {
			return r.lastOut
		}
		nextWrapped = r.sched.Next(r.lastWrapped)
	}

	// Get a random number of seconds to offset.
	sec := int64(r.offset / time.Second)
	randOffset := time.Duration(rand.Int63n(2*sec)-sec) * time.Second

	r.lastWrapped = nextWrapped
	r.lastOut = r.lastWrapped.Add(randOffset)

	// Always go forwards
	for !r.lastOut.After(t) {
		r.lastWrapped = r.sched.Next(r.lastWrapped)
		r.lastOut = r.lastWrapped.Add(randOffset)
	}

	return r.lastOut
}

// sunrise holds the lat and long coordinates for calculating sunrise time.
type sunrise struct {
	lat, long float64
	y, d      int
	m         time.Month
}

// NewSunrise returns a Schedule of sunrises.
func NewSunrise(lat, long float64) cron.Schedule {
	return &sunrise{lat: lat, long: long}
}

// Next returns the time of the next sunrise after t. It satisfies the Schedule
// interface.
func (s *sunrise) Next(t time.Time) time.Time {
	// astrotime.NextSunset can return times on the same day.
	tDay := t.Truncate(time.Hour * 24).Add(time.Second)
	next := astrotime.CalcSunrise(tDay, s.lat, s.long)

	for !next.After(t) {
		tDay = tDay.Add(24 * time.Hour)
		next = astrotime.CalcSunrise(tDay, s.lat, s.long)
	}

	return next.Truncate(time.Second)
}

// sunset hold the lat and long coordinates for calculating sunset time.
type sunset struct {
	lat, long float64
}

// NewSunset returns a Schedule of sunsets.
func NewSunset(lat, long float64) cron.Schedule {
	return &sunset{lat: lat, long: long}
}

// Next returns the time of the next sunset after t. It satisfies the Schedule
// interface.
func (s *sunset) Next(t time.Time) time.Time {
	// astrotime.NextSunset can return times on the same day.
	tDay := t.Truncate(time.Hour * 24).Add(time.Second)
	next := astrotime.CalcSunset(tDay, s.lat, s.long)

	for !next.After(t) {
		tDay = tDay.Add(24 * time.Hour)
		next = astrotime.CalcSunset(tDay, s.lat, s.long)
	}

	return next.Truncate(time.Second)
}

// NewSubtract returns a schedule which subtracts a specific offset from the
// wrapped schedule.
func NewSubtract(offset time.Duration, sched cron.Schedule) cron.Schedule {
	return &add{offset: -offset, sched: sched}
}

// add holds the offset and wrapped Schedule.
type add struct {
	offset time.Duration
	sched  cron.Schedule
}

// NewAdd returns a schedule which adds a specific offset to the wrapped
// schedule.
func NewAdd(offset time.Duration, sched cron.Schedule) cron.Schedule {
	return &add{offset: offset, sched: sched}
}

// Next returns the next time of the wrapped schedule plus the configured
// offset.
func (s *add) Next(t time.Time) time.Time {
	next := s.sched.Next(t)

	// Offset can be negative, and output must always be after t.
	for !next.Add(s.offset).After(t) {
		next = s.sched.Next(next)
	}

	return next.Add(s.offset)
}

// Suppress hold the current start and duration of the suppression, and the
// wrapped schedule.
type Suppress struct {
	start time.Time
	dur   time.Duration
	sched cron.Schedule
}

// NewEmptySuppress returns an empty suppress schedule which will pass through
// the wrapped schedule. Use the Suppress method on the returned struct to set a
// time and duration to skip.
func NewEmptySuppress(sched cron.Schedule) *Suppress {
	return &Suppress{sched: sched}
}

// NewSuppress returns a suppress schedule which will skip any instances during
// the configured interval. The Suppress method on the returned struct to change
// or clear the time and duration to skip.
func NewSuppress(start time.Time, dur time.Duration, sched cron.Schedule) *Suppress {
	s := NewEmptySuppress(sched)
	s.Suppress(start, dur)
	return s
}

// Set a start time and duration to suppress triggers of the wrapped schedule.
// Sending the zero values will clear the suppression.
func (s *Suppress) Suppress(start time.Time, dur time.Duration) {
	s.start, s.dur = start, dur
}

// Next returns the next unsuppressed time of the wrapped schedule.
func (s *Suppress) Next(t time.Time) time.Time {
	t = s.sched.Next(t)
	if s.start != (time.Time{}) && s.dur != 0 {
		for t.After(s.start) && t.Before(s.start.Add(s.dur)) {
			t = s.sched.Next(t.Add(time.Second))
		}
	}
	return t
}

// Union holds the wrapped schedules.
type Union struct {
	scheds []cron.Schedule
}

// NewUnion returns a union schedule which will report the next time when all
// provided schedules are active. It's recommended to provide schedules from
// most to least restrictive for efficiency, although any order will work.
// Union will give up if there is no overlap in the next 32 days (32 * 24h).
func NewUnion(scheds ...cron.Schedule) (cron.Schedule, error) {
	if len(scheds) < 2 {
		return nil, fmt.Errorf("NewUnion needs at least 2 schedules, got %d", len(scheds))
	}
	return &Union{scheds: scheds}, nil
}

// Next returns the next time when all schedules are active.
func (u *Union) Next(t time.Time) time.Time {
	candidate := u.scheds[0].Next(t)

	maxT := t.Add(32 * 24 * time.Hour) // Search up to 32 days
newCandidate:
	for t.Before(maxT) {
		for _, s := range u.scheds {
			tmpT := t
			for s.Next(tmpT).Before(candidate) {
				tmpT = s.Next(tmpT)
			}
			if s.Next(tmpT).After(candidate) {
				// This schedule passed the candidate wihtout hitting it.
				t = candidate                           // Move the start time forward.
				candidate = u.scheds[0].Next(candidate) // Move the candidate forward.

				continue newCandidate
			}
		}

		// All schedules matched the candidate!
		break
	}

	if t.Before(maxT) {
		return candidate
	}
	return time.Time{}
}

// jsonTypes maps a schedule type name to it's required fields.
var jsonTypes = map[string][]string{
	"spec":     []string{"spec"},
	"random":   []string{"offset", "wrapped"},
	"sunrise":  []string{"latitude", "longitude"},
	"sunset":   []string{"latitude", "longitude"},
	"subtract": []string{"offset", "wrapped"},
	"add":      []string{"offset", "wrapped"},
	"suppress": []string{"wrapped", "start", "duration"},
	"union":    []string{"union"},
}

// jsonScehdule represents a JSON Serialized form of an arbitrary schedule.
type jsonSchedule struct {
	Name string
	Type string
	Spec string

	Wrapped       *jsonSchedule
	parsedWrapped cron.Schedule

	Elements       []*jsonSchedule
	parsedElements []cron.Schedule

	Offset       string
	parsedOffset time.Duration

	Latitude  float64
	Longitude float64

	Start       string
	parsedStart time.Time

	Duration       string
	parsedDuration time.Duration
}

// ParseJSON takes a JSON serialized schedule, parses it, validates it, and
// returns a cron.Schedule.
func ParseJSON(blob []byte) (string, cron.Schedule, error) {
	js := jsonSchedule{}
	if err := json.Unmarshal(blob, &js); err != nil {
		return "", nil, err
	}

	s, err := jsonHelper(&js, true)
	if err != nil {
		return "", nil, err
	}
	return js.Name, s, nil
}

func jsonHelper(js *jsonSchedule, named bool) (cron.Schedule, error) {
	fields, ok := jsonTypes[strings.ToLower(js.Type)]
	if !ok {
		return nil, fmt.Errorf("unknown schedule type %q", js.Type)
	}

	if named && js.Name == "" {
		return nil, errors.New("first schedule must be named")
	} else if !named && js.Name != "" {
		return nil, errors.New("nested schedules must not be named")
	}

	for _, f := range fields {
		switch f {
		case "spec":
			if js.Spec == "" {
				return nil, fmt.Errorf("field %q missing from schedule %#v", f, *js)
			}
		case "wrapped":
			if js.Wrapped == nil {
				return nil, fmt.Errorf("field %q missing from schedule %#v", f, *js)
			}
			s, err := jsonHelper(js.Wrapped, false)
			if err != nil {
				return nil, err
			}
			js.parsedWrapped = s
		case "elements":
			if len(js.Elements) < 2 {
				return nil, fmt.Errorf("%q must contain at least two sub-scheduled (%d found)", f, len(js.Elements))
			}
			for _, subJS := range js.Elements {
				s, err := jsonHelper(subJS, false)
				if err != nil {
					return nil, err
				}
				js.parsedElements = append(js.parsedElements, s)
			}
		case "offset":
			if js.Offset == "" {
				return nil, fmt.Errorf("field %q missing from schedule %#v", f, *js)
			}
			d, err := time.ParseDuration(js.Offset)
			if err != nil {
				return nil, fmt.Errorf("invalid %q: %v", js.Offset, err)
			}
			js.parsedOffset = d
		case "longitude":
			if js.Longitude == 0 {
				return nil, fmt.Errorf("field %q missing from schedule %#v", f, *js)
			}
		case "latitude":
			if js.Latitude == 0 {
				return nil, fmt.Errorf("field %q missing from schedule %#v", f, *js)
			}
		case "start":
			if js.Start == "" {
				// optional suppress field
				break
			}
			t, err := time.Parse(time.RFC3339, js.Start)
			if err != nil {
				return nil, fmt.Errorf("invalid %q: %v", f, err)
			}
			js.parsedStart = t
		case "duration":
			if js.Duration == "" {
				// optional suppress field
				break
			}
			d, err := time.ParseDuration(js.Duration)
			if err != nil {
				return nil, fmt.Errorf("invalid %q: %v", js.Duration, err)
			}
			js.parsedDuration = d
		default:
			return nil, fmt.Errorf("unknown field %f", f)
		}
	}

	var s cron.Schedule
	switch js.Type {
	case "spec":
		var err error
		if s, err = cron.Parse(js.Spec); err != nil {
			return nil, err
		}
	case "random":
		s = NewRandom(js.parsedOffset, js.parsedWrapped)
	case "sunrise":
		s = NewSunrise(js.Latitude, js.Longitude)
	case "sunset":
		s = NewSunset(js.Latitude, js.Longitude)
	case "subtract":
		s = NewSubtract(js.parsedOffset, js.parsedWrapped)
	case "add":
		s = NewAdd(js.parsedOffset, js.parsedWrapped)
	case "suppress":
		if js.Start != "" && js.Duration != "" {
			s = NewSuppress(js.parsedStart, js.parsedDuration, js.parsedWrapped)
		} else {
			s = NewEmptySuppress(js.parsedWrapped)
		}
	case "union":
		var err error
		if s, err = NewUnion(js.parsedElements...); err != nil {
			return nil, err
		}
	}

	return s, nil
}

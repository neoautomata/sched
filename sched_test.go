/*
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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/robfig/cron"
)

var (
	pitLat     float64 = 40.440624
	pitLong    float64 = 79.995888
	pitLoc, _          = time.LoadLocation("America/New_York")
	pitTime            = time.Date(2017, 3, 21, 5, 0, 0, 0, pitLoc)
	pitSunrise         = time.Date(2017, 3, 21, 07, 21, 13, 0, pitLoc)
	pitSunset          = time.Date(2017, 3, 21, 19, 33, 26, 0, pitLoc)
)

func TestRandom(t *testing.T) {
	testCases := []struct {
		name             string
		wrapSpec         string
		offset           time.Duration
		start            time.Time
		earliest, latest time.Duration
		iters            int
	}{
		{
			name:     "Every 15m with 10 min rand",
			wrapSpec: "@every 15m",
			offset:   10 * time.Minute,
			start:    pitTime,
			earliest: 5 * time.Minute,
			latest:   25 * time.Minute,
			iters:    100000,
		},
	}

	for _, tc := range testCases {
		wrapped, err := cron.Parse(tc.wrapSpec)
		if err != nil {
			t.Errorf("%s: cron.Parse(%q) = %s", tc.wrapSpec, err)
			continue
		}

		r := NewRandom(tc.offset, wrapped)
		nextIn := tc.start
		for i := 0; i < tc.iters; i++ {
			got := r.Next(nextIn)
			if got.Before(nextIn.Add(tc.earliest)) || got.After(nextIn.Add(tc.latest)) {
				t.Errorf("%s: random.Next(%v) = %v; want in range %v-%v", tc.name, nextIn, got, nextIn.Add(tc.earliest), nextIn.Add(tc.latest))
			}
			nextIn = wrapped.Next(nextIn)
		}
	}
}

func TestSunrise(t *testing.T) {
	testCases := []struct {
		name      string
		start     time.Time
		lat, long float64
		want      []time.Time
	}{
		{
			name:  "PIT sunrise",
			start: pitTime,
			lat:   pitLat,
			long:  pitLong,
			want:  []time.Time{pitSunrise},
		},
	}

	for _, tc := range testCases {
		s := NewSunrise(tc.lat, tc.long)
		nextIn := tc.start
		for _, w := range tc.want {
			got := s.Next(nextIn)
			if !got.Equal(w) {
				t.Errorf("%s: sunrise.Next(%v) = %v; want %v", tc.name, nextIn, got, w)
			}
			nextIn = got
		}
	}
}

func TestSunset(t *testing.T) {
	testCases := []struct {
		name      string
		start     time.Time
		lat, long float64
		want      []time.Time
	}{
		{
			name:  "PIT sunset",
			start: pitTime,
			lat:   pitLat,
			long:  pitLong,
			want:  []time.Time{pitSunset},
		},
	}

	for _, tc := range testCases {
		s := NewSunset(tc.lat, tc.long)
		nextIn := tc.start
		for _, w := range tc.want {
			got := s.Next(nextIn)
			if !got.Equal(w) {
				t.Errorf("%s: sunset.Next(%v) = %v; want %v", tc.name, nextIn, got, w)
			}
			nextIn = got
		}
	}
}

func TestSubtract(t *testing.T) {
	testCases := []struct {
		name     string
		wrapSpec string
		offset   time.Duration
		start    time.Time
		want     []time.Time
	}{
		{
			name:     "Every 15m - 5 min",
			wrapSpec: "@every 15m",
			offset:   5 * time.Minute,
			start:    pitTime,
			want: []time.Time{
				pitTime.Add(10 * time.Minute),
				pitTime.Add(2 * 10 * time.Minute),
				pitTime.Add(3 * 10 * time.Minute),
				pitTime.Add(4 * 10 * time.Minute),
				pitTime.Add(5 * 10 * time.Minute),
				pitTime.Add(6 * 10 * time.Minute),
			},
		},
		{
			// The output time MUST be after the input time.
			name:     "Every 15m - 20 min",
			wrapSpec: "@every 15m",
			offset:   20 * time.Minute,
			start:    pitTime,
			want: []time.Time{
				pitTime.Add(10 * time.Minute),
				pitTime.Add(2 * 10 * time.Minute),
				pitTime.Add(3 * 10 * time.Minute),
				pitTime.Add(4 * 10 * time.Minute),
				pitTime.Add(5 * 10 * time.Minute),
				pitTime.Add(6 * 10 * time.Minute),
			},
		},
	}

	for _, tc := range testCases {
		wrapped, err := cron.Parse(tc.wrapSpec)
		if err != nil {
			t.Errorf("%s: cron.Parse(%q) = %s", tc.wrapSpec, err)
			continue
		}

		s := NewSubtract(tc.offset, wrapped)
		nextIn := tc.start
		for _, w := range tc.want {
			got := s.Next(nextIn)
			if !got.Equal(w) {
				t.Errorf("%s: subtract.Next(%v) = %v; want %v", tc.name, nextIn, got, w)
			}
			nextIn = got
		}
	}
}

func TestAdd(t *testing.T) {
	testCases := []struct {
		name     string
		wrapSpec string
		offset   time.Duration
		start    time.Time
		want     []time.Time
	}{
		{
			name:     "Every 15m + 5 min",
			wrapSpec: "@every 15m",
			offset:   5 * time.Minute,
			start:    pitTime,
			want: []time.Time{
				pitTime.Add(20 * time.Minute),
				pitTime.Add(2 * 20 * time.Minute),
				pitTime.Add(3 * 20 * time.Minute),
				pitTime.Add(4 * 20 * time.Minute),
				pitTime.Add(5 * 20 * time.Minute),
				pitTime.Add(6 * 20 * time.Minute),
			},
		},
		{
			// The output time MUST be after the input time.
			name:     "Every 15m + 30 min",
			wrapSpec: "@every 15m",
			offset:   30 * time.Minute,
			start:    pitTime,
			want: []time.Time{
				pitTime.Add(45 * time.Minute),
				pitTime.Add(2 * 45 * time.Minute),
				pitTime.Add(3 * 45 * time.Minute),
				pitTime.Add(4 * 45 * time.Minute),
				pitTime.Add(5 * 45 * time.Minute),
				pitTime.Add(6 * 45 * time.Minute),
			},
		},
	}

	for _, tc := range testCases {
		wrapped, err := cron.Parse(tc.wrapSpec)
		if err != nil {
			t.Errorf("%s: cron.Parse(%q) = %s", tc.wrapSpec, err)
			continue
		}

		s := NewAdd(tc.offset, wrapped)
		nextIn := tc.start
		for _, w := range tc.want {
			got := s.Next(nextIn)
			if !got.Equal(w) {
				t.Errorf("%s: add.Next(%v) = %v; want %v", tc.name, nextIn, got, w)
			}
			nextIn = got
		}
	}
}

func TestSuppress(t *testing.T) {
	ws, err := cron.Parse("@every 15m")
	if err != nil {
		t.Errorf("cron.Parse('@every 15m') = %s", err)
	}

	s := NewEmptySuppress(ws)

	mins := (time.Until(s.Next(time.Now())) + 5*time.Second) / time.Minute
	if mins != 15 {
		t.Errorf("s.Next(now) = %d mins; want 15", mins)
	}

	s.Suppress(time.Now(), 16*time.Minute)
	mins = (time.Until(s.Next(time.Now())) + 5*time.Second) / time.Minute
	if mins != 30 {
		t.Errorf("s.Next(now) = %d mins; want 30", mins)
	}
}

func TestUnion(t *testing.T) {
	every15, err := cron.Parse("@every 15m")
	if err != nil {
		t.Errorf("cron.Parse('@every 15m') = %s", err)
	}

	wed, err := cron.Parse("* * * * * Wed")
	if err != nil {
		t.Errorf("cron.Parse('* * * * * Wed') = %s", err)
	}

	u, _ := NewUnion(wed, every15)
	uRev, _ := NewUnion(every15, wed)
	in := time.Date(2017, 3, 21, 12, 0, 0, 0, pitLoc)
	want := time.Date(2017, 3, 22, 0, 0, 0, 0, pitLoc)
	if got := u.Next(in); !got.Equal(want) {
		t.Errorf("u.Next(%v) = %v; want %v", in, got, want)
	}
	if got := uRev.Next(in); !got.Equal(want) {
		t.Errorf("uRev.Next(%v) = %v; want %v", in, got, want)
	}

	thu, err := cron.Parse("* * * * * Thu")
	if err != nil {
		t.Errorf("cron.Parse('* * * * * Thu') = %s", err)
	}

	impossible, _ := NewUnion(wed, thu)
	if got := impossible.Next(in); !got.Equal(time.Time{}) {
		t.Errorf("impossible.Next(%v) = %v; want %v", in, got, time.Time{})
	}
}

func TestParseJSON(t *testing.T) {
	every15m, _ := cron.Parse("@every 15m")
	testCases := []struct {
		name    string
		json    string
		want    cron.Schedule
		wantErr bool
	}{
		{
			name: "Spec",
			json: `{"name":"Spec test", "type": "spec", "spec": "@every 15m"}`,
			want: every15m,
		},
		{
			name: "Random",
			json: `{"name":"Random test", "type": "random", "offset": "15m", "wrapped": { "type": "spec", "spec": "@every 15m" } }`,
			want: NewRandom(15*time.Minute, every15m),
		},
		{
			name: "Sunrise",
			json: `{"name":"Sunrise test", "type": "sunrise", "latitude":` + fmt.Sprintf("%f", pitLat) + `, "longitude":` + fmt.Sprintf("%f", pitLong) + `}`,
			want: NewSunrise(pitLat, pitLong),
		},
		{
			name: "Sunset",
			json: `{"name":"Sunset test", "type": "sunset", "latitude":` + fmt.Sprintf("%f", pitLat) + `, "longitude":` + fmt.Sprintf("%f", pitLong) + `}`,
			want: NewSunset(pitLat, pitLong),
		},
		{
			name: "Subtract",
			json: `{"name":"Subtract test", "type": "subtract", "offset": "15m", "wrapped": { "type": "spec", "spec": "@every 15m" } }`,
			want: NewSubtract(15*time.Minute, every15m),
		},
		{
			name: "Add",
			json: `{"name":"Add test", "type": "add", "offset": "15m", "wrapped": { "type": "spec", "spec": "@every 15m" } }`,
			want: NewAdd(15*time.Minute, every15m),
		},
		{
			name: "Empty Suppress",
			json: `{"name":"Empty Suppress test", "type": "suppress", "wrapped": { "type": "spec", "spec": "@every 15m" } }`,
			want: NewEmptySuppress(every15m),
		},
		{
			name: "Suppress",
			json: `{"name":"Suppress test", "type": "suppress", "start":"` + pitTime.Format(time.RFC3339) + `", "duration":"3h", "wrapped": { "type": "spec", "spec": "@every 15m" } }`,
			want: NewSuppress(pitTime, 3*time.Hour, every15m),
		},
		{
			name: "Union",
			json: `{"name":"Union test", "type": "union", "elements": [ { "type": "spec", "spec": "@every 15m" }, { "type": "spec", "spec": "@every 30m" } ] }`,
			want: func() cron.Schedule {
				every30m, _ := cron.Parse("@every 30m")
				s, _ := NewUnion(every15m, every30m)
				return s
			}(),
		},
	}

	for _, tc := range testCases {
		_, got, err := ParseJSON([]byte(tc.json))
		if (err == nil) == tc.wantErr {
			t.Errorf("%s: ParseJSON() = %v; want err %t", tc.name, err, tc.wantErr)
		}
		if err != nil {
			continue
		}

		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: ParseJSON() = %#v\n\nwant: %#v", tc.name, got, tc.want)
		}
	}
}

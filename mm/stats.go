/*
   Copyright (c) 2014, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package mm

import (
	"fmt"
	"log"
	"sort"
)

type Stats struct {
	metricType string    `json:"-"` // ignore
	str        string    `json:",omitempty"`
	firstVal   bool      `json:"-"`
	prevTs     int64     `json:"-"`
	penuTs     int64     `json:"-"`
	prevVal    float64   `json:"-"` // last value
	penuVal    float64   `json:"-"` // 2nd to last (penultimate) value
	vals       []float64 `json:"-"`
	sum        float64   `json:"-"`
	Cnt        int
	Min        float64
	Pct5       float64
	Avg        float64
	Med        float64
	Pct95      float64
	Max        float64
}

func NewStats(metricType string) (*Stats, error) {
	if !MetricTypes[metricType] {
		return nil, fmt.Errorf("Invalid metric type: %s", metricType)
	}
	s := &Stats{
		metricType: metricType,
		vals:       []float64{},
		firstVal:   true,
	}
	return s, nil
}

func (s *Stats) Reset() {
	s.sum = 0
	s.vals = []float64{}
}

func (s *Stats) Add(m *Metric, ts int64) error {
	var err error
	switch s.metricType {
	case "gauge":
		s.vals = append(s.vals, m.Number)
		s.sum += m.Number
	case "counter":
		if !s.firstVal {
			if m.Number >= s.prevVal {
				// Metric value increased (or stayed same); this is what we expect.

				// https://jira.percona.com/browse/PCT-939
				if s.penuVal > 0 && s.prevVal == 0 && m.Number > s.penuVal {
					// @1 x=100
					// @2 x=0 (for whatever reason)
					// @3 x > 100
					// This means value reset then increased so quickly that it
					// lapped the previous non-zero value, which shouldn't happen;
					// or obvservation @2 was a blip and x should have been >100
					// && < @3. However, if the values are very small, it could
					// happen and could be legitimate, so for now we just return
					// an error to warn the caller.
					err = fmt.Errorf("Value lap: ts=%d val=%.6f, ts=%d val=%.6f, ts=%d val=%.6f",
						s.penuTs, s.penuVal, s.prevTs, s.prevVal, ts, m.Number)
				}

				// Per-second rate of value = increase / duration
				inc := m.Number - s.prevVal
				dur := ts - s.prevTs
				val := inc / float64(dur)
				s.vals = append(s.vals, val)

				// Keep running total to calc Avg.
				s.sum += val

				// Current values become previous values.
				s.penuTs = s.prevTs
				s.prevTs = ts
				s.penuVal = s.prevVal
				s.prevVal = m.Number
			} else {
				// Metric value reset, e.g. FLUSH GLOBAL STATUS.
				s.penuTs = s.prevTs
				s.prevTs = ts
				s.penuVal = s.prevVal
				s.prevVal = m.Number
			}
		} else {
			s.penuTs = s.prevTs
			s.prevTs = ts
			s.penuVal = s.prevVal
			s.prevVal = m.Number
			s.firstVal = false
		}
	default:
		// This should not happen because type is checked in NewStats().
		log.Panic("mm:Aggregator:Add: Invalid metric type: " + s.metricType)
	}
	return err
}

func (s *Stats) Finalize() *Stats {
	if len(s.vals) == 0 {
		return nil
	}
	s.Summarize()
	return &Stats{
		Cnt:   s.Cnt,
		Min:   s.Min,
		Pct5:  s.Pct5,
		Avg:   s.Avg,
		Med:   s.Med,
		Pct95: s.Pct95,
		Max:   s.Max,
	}
}

func (s *Stats) Summarize() {
	switch s.metricType {
	case "gauge", "counter":
		s.Cnt = len(s.vals)
		if s.Cnt > 1 {
			sort.Float64s(s.vals)
			s.Min = s.vals[0]
			s.Pct5 = s.vals[(5*s.Cnt)/100]
			s.Avg = s.sum / float64(s.Cnt)
			s.Med = s.vals[(50*s.Cnt)/100] // median = 50th percentile
			s.Pct95 = s.vals[(95*s.Cnt)/100]
			s.Max = s.vals[s.Cnt-1]
		} else if s.Cnt == 1 {
			s.Min = s.vals[0]
			s.Pct5 = s.vals[0]
			s.Avg = s.vals[0]
			s.Med = s.vals[0]
			s.Pct95 = s.vals[0]
			s.Max = s.vals[0]
		}
	}
}

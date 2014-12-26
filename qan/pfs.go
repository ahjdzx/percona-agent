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

package qan

import (
	"database/sql"
	"fmt"
	"github.com/percona/go-mysql/event"
	"github.com/percona/percona-agent/mysql"
	"github.com/percona/percona-agent/pct"
	"math"
	"strings"
	"time"
)

type PfsRow struct {
	Digest, DigestText                                         string
	SumTimerWait, MinTimerWait, AvgTimerWait, MaxTimerWait     uint64
	SumLockTime, SumRowsAffected, SumRowsSent, SumRowsExamined uint64
	SumSelectFullJoin, SumSelectScan, SumSortMergePasses       uint
	SumCreatedTmpDiskTables, SumCreatedTmpTables, CountStar    uint
	FirstSeen, LastSeen                                        time.Time
}

type PfsWorker struct {
	logger    *pct.Logger
	name      string
	mysqlConn mysql.Connector
	status    *pct.Status
}

func NewPfsWorker(logger *pct.Logger, name string, mysqlConn mysql.Connector) *PfsWorker {
	w := &PfsWorker{
		logger:    logger,
		name:      name,
		mysqlConn: mysqlConn,
		status:    pct.NewStatus([]string{name}),
	}
	return w
}

func (w *PfsWorker) Name() string {
	return w.name
}

func (w *PfsWorker) Status() string {
	return w.status.Get(w.name)
}

func (w *PfsWorker) Run(job *Job) (*Result, error) {
	defer w.status.Update(w.name, "Idle")

	w.status.Update(w.name, "Connecting to MySQL")
	if err := w.mysqlConn.Connect(2); err != nil {
		return nil, err
	}
	defer w.mysqlConn.Close()

	rows, err := w.CollectData()
	if err != nil {
		return nil, err
	}
	if err := w.TruncateTable(); err != nil {
		return nil, err
	}
	return w.PrepareResult(rows)
}

func (w *PfsWorker) CollectData() ([]*PfsRow, error) {
	w.status.Update(w.name, "SELECT performance_schema.events_statements_summary_by_digest")

	query := "SELECT " +
		"DIGEST, DIGEST_TEXT, COUNT_STAR, " +
		"SUM_TIMER_WAIT, MIN_TIMER_WAIT, AVG_TIMER_WAIT, MAX_TIMER_WAIT, " +
		"SUM_LOCK_TIME, SUM_ROWS_AFFECTED, SUM_ROWS_SENT, SUM_ROWS_EXAMINED, " +
		"SUM_CREATED_TMP_DISK_TABLES, SUM_CREATED_TMP_TABLES, " +
		"SUM_SELECT_FULL_JOIN, SUM_SELECT_SCAN,SUM_SORT_MERGE_PASSES, " +
		"FIRST_SEEN, LAST_SEEN " +
		"FROM performance_schema.events_statements_summary_by_digest"
	rows, err := w.mysqlConn.DB().Query(query)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	data := []*PfsRow{}
	for rows.Next() {
		row := &PfsRow{}
		err := rows.Scan(
			&row.Digest, &row.DigestText, &row.CountStar,
			&row.SumTimerWait, &row.MinTimerWait, &row.AvgTimerWait, &row.MaxTimerWait,
			&row.SumLockTime, &row.SumRowsAffected, &row.SumRowsSent, &row.SumRowsExamined,
			&row.SumCreatedTmpDiskTables, &row.SumCreatedTmpTables,
			&row.SumSelectFullJoin, &row.SumSelectScan, &row.SumSortMergePasses,
			&row.FirstSeen, &row.LastSeen,
		)
		if err != nil {
			return nil, fmt.Errorf("rows.Scan error: %s: ", err)
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err error: %s: ", err)
	}
	return data, nil
}

func (w *PfsWorker) TruncateTable() error {
	w.status.Update(w.name, "TRUNCATE performance_schema.events_statements_summary_by_digest")
	_, err := w.mysqlConn.DB().Exec("TRUNCATE performance_schema.events_statements_summary_by_digest")
	return err
}

func (w *PfsWorker) PrepareResult(rows []*PfsRow) (*Result, error) {
	w.status.Update(w.name, "Preparing result")

	global := event.NewGlobalClass()
	classes := []*event.QueryClass{}
	for _, row := range rows {
		// Each row is a pre-aggregated query class, so all we have to do is save
		// the stats for the available metrics.  Unlike events from a slow log,
		// these values do not need to be aggregated or finalized because they
		// already are.
		stats := event.NewMetrics()
		cnt := row.CountStar

		// Time metircs, in picoseconds (x10^-12 to convert to seconds)
		stats.TimeMetrics["Query_time"] = &event.TimeStats{
			Cnt: cnt,
			Sum: float64(row.SumTimerWait) * math.Pow10(-12),
			Min: float64(row.MinTimerWait) * math.Pow10(-12),
			Max: float64(row.MaxTimerWait) * math.Pow10(-12),
			Avg: float64(row.AvgTimerWait) * math.Pow10(-12),
		}

		stats.TimeMetrics["Lock_time"] = &event.TimeStats{
			Cnt: cnt,
			Sum: float64(row.SumLockTime) * math.Pow10(-12),
		}

		// Number metrics
		stats.NumberMetrics["Rows_affected"] = &event.NumberStats{
			Cnt: cnt,
			Sum: row.SumRowsAffected,
		}

		stats.NumberMetrics["Rows_sent"] = &event.NumberStats{
			Cnt: cnt,
			Sum: row.SumRowsSent,
		}

		stats.NumberMetrics["Rows_examined"] = &event.NumberStats{
			Cnt: cnt,
			Sum: row.SumRowsExamined,
		}

		stats.NumberMetrics["Merge_passes"] = &event.NumberStats{
			Cnt: cnt,
			Sum: uint64(row.SumSortMergePasses),
		}

		// Bool metrics
		stats.BoolMetrics["Tmp_table_on_disk"] = &event.BoolStats{
			Cnt:  cnt,
			True: row.SumCreatedTmpDiskTables,
		}

		stats.BoolMetrics["Tmp_table"] = &event.BoolStats{
			Cnt:  cnt,
			True: row.SumCreatedTmpTables,
		}

		stats.BoolMetrics["Full_join"] = &event.BoolStats{
			Cnt:  cnt,
			True: row.SumSelectFullJoin,
		}

		stats.BoolMetrics["Full_scan"] = &event.BoolStats{
			Cnt:  cnt,
			True: row.SumSelectScan,
		}

		// Create and save the pre-aggregated class.  Using only last 16 digits
		// of checksum is historical: pt-query-digest does the same:
		// my $checksum = uc substr(md5_hex($val), -16);
		classId := strings.ToUpper(row.Digest[16:32])
		class := event.NewQueryClass(classId, row.DigestText, false)
		class.TotalQueries = uint64(row.CountStar)
		class.Metrics = stats
		classes = append(classes, class)

		// Add the class to the global metrics.
		global.AddClass(class)
	}

	// Each row/class was unique, so update the global counts.
	nClasses := uint64(len(classes))
	global.TotalQueries = nClasses
	global.UniqueQueries = nClasses

	result := &Result{
		Global: global,
		Class:  classes,
	}

	return result, nil
}

package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/castaneai/spandbcompare"

	"github.com/fatih/color"
)

const (
	datetimeFormat = "2006-01-02 15:04:05.999999Z07:00"
	colorAdded     = color.FgHiGreen
	colorDeleted   = color.FgRed
)

func colfmt(cols []string) string {
	longestColumnChars := 0
	for _, cn := range cols {
		if len(cn) > longestColumnChars {
			longestColumnChars = len(cn)
		}
	}
	return fmt.Sprintf("%%%d.%ds", longestColumnChars, longestColumnChars)
}

func currentTz() *time.Location {
	name, offset := time.Now().Zone()
	return time.FixedZone(name, offset)
}

func fmtval(v spandbcompare.ColumnValue) string {
	// Timestamp 型は format, timezone を統一して表示
	if v == nil {
		return "<NULL>"
	}
	switch v.(type) {
	case int, *int, uint, *uint, int8, *int8, uint8, *uint8, int16, *int16, uint16, *uint16, int32, *int32, uint32, *uint32, int64, *int64, uint64, *uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64, *float32, *float64:
		return fmt.Sprintf("%f", v)
	case time.Time:
		loc := currentTz()
		return v.(time.Time).In(loc).Format(datetimeFormat)
	}
	return strings.Replace(fmt.Sprintf("%v", v), "\n", "\\n", -1)
}

type UnifiedDiff struct {
	w    io.Writer
	cols []string
}

func NewUnifiedDiff(w io.Writer, cols []string) (*UnifiedDiff, error) {
	return &UnifiedDiff{
		w:    w,
		cols: cols,
	}, nil
}

func (ud *UnifiedDiff) printf(format string, a ...interface{}) {
	fmt.Fprintf(ud.w, format, a...)
}

func (ud *UnifiedDiff) Write(diff *spandbcompare.TablesDiff, changesFor string) error {
	var before, after string
	var added, deleted []*spandbcompare.Row
	switch changesFor {
	case diff.Table1:
		before = diff.Table1
		after = diff.Table2
		added = diff.RowsDiff.Rows2Only
		deleted = diff.RowsDiff.Rows1Only
		break
	case diff.Table2:
		before = diff.Table2
		after = diff.Table1
		added = diff.RowsDiff.Rows1Only
		deleted = diff.RowsDiff.Rows2Only
		break
	default:
		return fmt.Errorf("changesFor must be %s or %s", diff.Table1, diff.Table2)
	}

	if err := ud.WriteUpdated(before, after, diff.RowsDiff.DiffRows); err != nil {
		return err
	}
	if err := ud.WriteAdded(added); err != nil {
		return err
	}
	if err := ud.WriteDeleted(deleted); err != nil {
		return err
	}
	return nil
}

func (ud *UnifiedDiff) WriteAdded(rows []*spandbcompare.Row) error {
	added := color.New(colorAdded).FprintfFunc()
	cfmt := colfmt(ud.cols)

	for i, row := range rows {
		ud.printf(" ************************* %5d. row *************************\n", i)
		for _, cn := range ud.cols {
			added(ud.w, "+ "+cfmt+": %s\n", cn, fmtval(row.ColumnValues[cn]))
		}
	}
	ud.printf("  %d rows\n\n", len(rows))
	return nil
}

func (ud *UnifiedDiff) WriteDeleted(rows []*spandbcompare.Row) error {
	deleted := color.New(colorDeleted).FprintfFunc()
	cfmt := colfmt(ud.cols)

	for i, row := range rows {
		ud.printf(" ************************* %5d. row *************************\n", i)
		for _, cn := range ud.cols {
			deleted(ud.w, "- "+cfmt+": %s\n", cn, fmtval(row.ColumnValues[cn]))
		}
	}
	ud.printf("  %d rows\n\n", len(rows))
	return nil
}

func (ud *UnifiedDiff) WriteUpdated(before, after string, rows []*spandbcompare.RowDiff) error {
	deleted := color.New(colorDeleted).FprintfFunc()
	added := color.New(colorAdded).FprintfFunc()
	cfmt := colfmt(ud.cols)

	deleted(ud.w, "--- %s\n", before)
	added(ud.w, "+++ %s\n", after)

	for i, rd := range rows {
		ud.printf(" ************************* %5d. row *************************\n", i)
		for cn, cv1 := range rd.Row1.ColumnValues {
			ispk := false
			for _, pkcn := range rd.Row1.PKCols {
				if cn == pkcn {
					ud.printf("  "+cfmt+": %s\n", cn, fmtval(cv1))
					ispk = true
					break
				}
			}
			if ispk {
				continue
			}

			cv2, ok := rd.Row2.ColumnValues[cn]
			if !ok {
				return fmt.Errorf("row1[%s] exists, but row2[%s] not found", cn, cn)
			}
			deleted(ud.w, "- "+cfmt+": %s\n", cn, fmtval(cv1))
			added(ud.w, "+ "+cfmt+": %s\n", cn, fmtval(cv2))
		}
	}
	ud.printf("  %d rows\n\n", len(rows))
	return nil
}

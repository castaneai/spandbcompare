package pkg

import (
	"fmt"
	"io"
	"strings"
	"time"

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

func fmtval(v ColumnValue) string {
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
	w          io.Writer
	cols       []string
	rows1Label string
	rows2Label string
}

func NewUnifiedDiff(w io.Writer, cols []string, rows1Label, rows2Label string) (*UnifiedDiff, error) {
	return &UnifiedDiff{
		w:          w,
		cols:       cols,
		rows1Label: rows1Label,
		rows2Label: rows2Label,
	}, nil
}

func (ud *UnifiedDiff) printf(format string, a ...interface{}) {
	fmt.Fprintf(ud.w, format, a...)
}

func (ud *UnifiedDiff) Write(rd *RowsDiff, changesFor string) error {
	if err := ud.validateChangesFor(changesFor); err != nil {
		return err
	}

	before, after := ud.rows1Label, ud.rows2Label
	rowsAdded, rowsDeleted := rd.Rows2Only, rd.Rows1Only
	if changesFor == ud.rows2Label {
		before, after = after, before
		rowsAdded, rowsDeleted = rowsDeleted, rowsAdded
	}

	deleted := color.New(colorDeleted).FprintfFunc()
	added := color.New(colorAdded).FprintfFunc()
	deleted(ud.w, "--- %s\n", before)
	added(ud.w, "+++ %s\n", after)

	if !rd.HasDiff() {
		ud.printf("No diff found\n\n")
		return nil
	}

	if err := ud.WriteUpdated(before, after, rd.DiffRows); err != nil {
		return err
	}
	if err := ud.WriteAdded(rowsAdded); err != nil {
		return err
	}
	if err := ud.WriteDeleted(rowsDeleted); err != nil {
		return err
	}
	return nil
}

func (ud *UnifiedDiff) validateChangesFor(changesFor string) error {
	if changesFor != ud.rows1Label && changesFor != ud.rows2Label {
		return fmt.Errorf("chnagesFor must be '%s' or '%s'", ud.rows1Label, ud.rows2Label)
	}
	return nil
}

func (ud *UnifiedDiff) WriteAdded(rows []*Row) error {
	added := color.New(colorAdded).FprintfFunc()
	cfmt := colfmt(ud.cols)

	for i, row := range rows {
		ud.printf(" ************************* %5d. row *************************\n", i)
		for _, cn := range ud.cols {
			added(ud.w, "+ "+cfmt+": %s\n", cn, fmtval(row.ColumnValues[cn]))
		}
	}
	ud.printf("\n %d rows added\n\n", len(rows))
	return nil
}

func (ud *UnifiedDiff) WriteDeleted(rows []*Row) error {
	deleted := color.New(colorDeleted).FprintfFunc()
	cfmt := colfmt(ud.cols)

	for i, row := range rows {
		ud.printf(" ************************* %5d. row *************************\n", i)
		for _, cn := range ud.cols {
			deleted(ud.w, "- "+cfmt+": %s\n", cn, fmtval(row.ColumnValues[cn]))
		}
	}
	ud.printf("\n %d rows deleted\n\n", len(rows))
	return nil
}

func (ud *UnifiedDiff) WriteUpdated(before, after string, rows []*RowDiff) error {
	deleted := color.New(colorDeleted).FprintfFunc()
	added := color.New(colorAdded).FprintfFunc()
	cfmt := colfmt(ud.cols)

	for i, rd := range rows {
		ud.printf(" ************************* %5d. row *************************\n", i)
		for _, cn := range ud.cols {
			cv1, ok := rd.Row1.ColumnValues[cn]
			if !ok {
				return fmt.Errorf("key: %v not found on row", cn)
			}
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
	ud.printf("\n %d rows updated\n\n", len(rows))
	return nil
}

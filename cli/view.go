package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/castaneai/spancompare"

	"github.com/fatih/color"
)

const (
	datetimeFormat = "2006-01-02T15:04:05.999999Z07:00"
	colorAdded     = color.FgHiGreen
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

func fmtval(v spancompare.ColumnValue) string {
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

type DiffViewer interface {
	Write(w io.Writer, cols []string, rows []*spancompare.Row) error
}

type DiffAdded struct{}

func (d *DiffAdded) Write(w io.Writer, cols []string, rows []*spancompare.Row) error {
	added := color.New(colorAdded).FprintfFunc()
	cfmt := colfmt(cols)

	for i, row := range rows {
		fmt.Fprintf(w, " ************************* %5d. row *************************\n", i)
		for _, cn := range cols {
			added(w, "+ "+cfmt+": %s\n", cn, fmtval(row.ColumnValues[cn]))
		}
	}
	fmt.Fprintf(w, "  %d rows\n", len(rows))
	fmt.Fprintln(w)
	return nil
}

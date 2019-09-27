package spandbcompare

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type SQLDiff struct {
	RowsDiff *RowsDiff
	Rows1Table string
	Rows2Table string
}

func (sd *SQLDiff) SQL(changesFor string) ([]string, error) {
	if err := sd.validateChangesFor(changesFor); err != nil {
		return nil, err
	}

	var sqls []string
	rowsAdded := sd.RowsDiff.Rows2Only
	rowsDeleted := sd.RowsDiff.Rows1Only
	if changesFor == sd.Rows2Table {
		rowsAdded, rowsDeleted = rowsDeleted, rowsAdded
	}

	sqls = append(sqls, insertSQL(changesFor, rowsAdded)...)
	var updateRows []*Row
	for _, rd := range sd.RowsDiff.DiffRows {
		updateRow := rd.Row2
		if changesFor == sd.Rows2Table {
			updateRow = rd.Row1
		}
		updateRows = append(updateRows, updateRow)
	}
	sqls = append(sqls, updateSQL(changesFor, updateRows)...)
	sqls = append(sqls, deleteSQL(changesFor, rowsDeleted)...)
	return sqls, nil
}

func (sd *SQLDiff) validateChangesFor(changesFor string) error {
	if changesFor != sd.Rows1Table && changesFor != sd.Rows2Table {
		return fmt.Errorf("chnagesFor must be '%s' or '%s'", sd.Rows1Table, sd.Rows2Table)
	}
	return nil
}

func insertSQL(table string, rows []*Row) []string {
	if len(rows) < 1 {
		return nil
	}

	var cols []string
	var qcols []string
	for cn, _ := range rows[0].ColumnValues {
		cols = append(cols, cn)
		qcols = append(qcols, fmt.Sprintf("`%s`", cn))
	}
	sort.Strings(cols)
	sort.Strings(qcols)

	var vals []string
	for _, row := range rows {
		var valss []string
		for _, cn := range cols {
			valss = append(valss, fmt.Sprintf("%s", literal(row.ColumnValues[cn])))
		}
		vals = append(vals, fmt.Sprintf("(%s)", strings.Join(valss, ",")))
	}
	return []string{fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", table, strings.Join(qcols, ","), strings.Join(vals, ","))}
}

func updateSQL(table string, rows []*Row) []string {
	if len(rows) < 1 {
		return nil
	}

	var sqls []string
	for _, row := range rows {
		var wheres []string
		for _, pkn := range row.PKCols {
			wheres = append(wheres, fmt.Sprintf("`%s` = %s", pkn, literal(row.ColumnValues[pkn])))
		}

		var sets []string
		for cn, cv := range row.ColumnValues {
			skip := false
			for _, pkn := range row.PKCols {
				if cn == pkn {
					skip = true
				}
			}
			if !skip {
				sets = append(sets, fmt.Sprintf("`%s` = %s", cn, literal(cv)))
			}
		}
		sqls = append(sqls, fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", table, strings.Join(sets, ","), strings.Join(wheres, " and ")))
	}
	return sqls
}

func deleteSQL(table string, rows []*Row) []string {
	if len(rows) < 1 {
		return nil
	}

	var sqls []string
	for _, row := range rows {
		var wheres []string
		for _, pkn := range row.PKCols {
			wheres = append(wheres, fmt.Sprintf("`%s` = %s", pkn, literal(row.ColumnValues[pkn])))
		}
		sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s` WHERE %s", table, strings.Join(wheres, " and ")))
	}
	return sqls
}

func literal(cv ColumnValue) string {
	switch cv.(type) {
	case string, *string:
		return fmt.Sprintf("'%s'", cv)
	case time.Time:
		return fmt.Sprintf("'%s'", cv.(time.Time).Format(time.RFC3339Nano))
	case float32, float64, *float32, *float64:
		// https://stackoverflow.com/questions/48337330/how-to-print-float-as-string-in-golang-without-scientific-notation
		return fmt.Sprintf("%f", cv)
	default:
		return fmt.Sprintf("%v", cv)
	}
}

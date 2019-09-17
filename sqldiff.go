package spancompare

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func (td *TablesDiff) SQL(changesFor string) ([]string, error) {
	var sqls []string
	switch changesFor {
	case td.Table1:
		sqls = append(sqls, insertSQL(td.Table1, td.RowsDiff.Rows2Only)...)
		var updateRows []*Row
		for _, rd := range td.RowsDiff.DiffRows {
			updateRows = append(updateRows, rd.Row2)
		}
		sqls = append(sqls, updateSQL(td.Table1, updateRows)...)
		sqls = append(sqls, deleteSQL(td.Table1, td.RowsDiff.Rows1Only)...)
	case td.Table2:
		sqls = append(sqls, insertSQL(td.Table1, td.RowsDiff.Rows1Only)...)
		var updateRows []*Row
		for _, rd := range td.RowsDiff.DiffRows {
			updateRows = append(updateRows, rd.Row1)
		}
		sqls = append(sqls, updateSQL(td.Table1, updateRows)...)
		sqls = append(sqls, deleteSQL(td.Table1, td.RowsDiff.Rows2Only)...)
	default:
		return nil, fmt.Errorf("changesFor must be '%s' or '%s", td.Table1, td.Table2)
	}
	return sqls, nil
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

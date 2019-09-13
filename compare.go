package spanner_compare

import (
	"errors"
	"fmt"
	"reflect"
)

type Result interface {
}

type Comparator interface {
	Compare(a, b interface{}) bool
}

type AsStringComparator struct{}

func (cmp *AsStringComparator) Compare(a, b interface{}) bool {
	return fmt.Sprintf("%s", a) == fmt.Sprintf("%s", b)
}

type Diff struct {
	Rows1Only []*Row
	Rows2Only []*Row
	Rows1     []*Row
	Rows2     []*Row
}

func (d *Diff) HasDiff() bool {
	return len(d.Rows1Only) > 0 || len(d.Rows2Only) > 0 || len(d.Rows1) > 0 || len(d.Rows2) > 0
}

func Compare(rows1, rows2 []*Row, cmp Comparator) (*Diff, error) {
	rows1Map := rowsToPKMap(rows1)
	rows2Map := rowsToPKMap(rows2)

	diff := &Diff{}
	for pks, row1 := range rows1Map {
		row2, exists2 := rows2Map[pks]
		if !exists2 {
			diff.Rows1Only = append(diff.Rows1Only, row1)
			continue
		}
		irow1, irow2, err := intersect(row1, row2, cmp)
		if err != nil {
			return nil, err
		}
		if irow1 != nil && irow2 != nil {
			diff.Rows1 = append(diff.Rows1, irow1)
			diff.Rows2 = append(diff.Rows2, irow2)
		}
	}
	for pks, row2 := range rows2Map {
		if _, exists1 := rows1Map[pks]; !exists1 {
			diff.Rows2Only = append(diff.Rows2Only, row2)
		}
	}
	return diff, nil
}

func intersect(row1, row2 *Row, cmp Comparator) (*Row, *Row, error) {
	irow1 := &Row{
		pkColNames:   row1.pkColNames,
		ColumnValues: make(map[string]ColumnValue),
	}
	irow2 := &Row{
		pkColNames:   row1.pkColNames,
		ColumnValues: make(map[string]ColumnValue),
	}
	if !reflect.DeepEqual(irow1.pkColNames, irow2.pkColNames) {
		return nil, nil, errors.New("the primary key of pair of rows must be the same")
	}
	// always includes the primary keys
	for _, pkcn := range irow1.pkColNames {
		irow1.ColumnValues[pkcn] = row1.ColumnValues[pkcn]
		irow2.ColumnValues[pkcn] = row2.ColumnValues[pkcn]
	}
	for cn, cv1 := range row1.ColumnValues {
		cv2, exists2 := row2.ColumnValues[cn]
		if !exists2 {
			irow1.ColumnValues[cn] = cv1
			continue
		}
		if !cmp.Compare(cv1, cv2) {
			irow1.ColumnValues[cn] = cv1
			irow2.ColumnValues[cn] = cv2
		}
	}
	// check whether column value has diff excluding primary key
	if len(irow1.ColumnValues) <= len(irow1.pkColNames) || len(irow2.ColumnValues) <= len(irow2.pkColNames) {
		return nil, nil, nil
	}
	return irow1, irow2, nil
}

func rowsToPKMap(rows []*Row) map[string]*Row {
	pkmap := make(map[string]*Row, len(rows))
	for _, row := range rows {
		pks := row.PrimaryKey().String()
		pkmap[pks] = row
	}
	return pkmap
}

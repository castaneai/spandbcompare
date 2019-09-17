package spancompare

import (
	"errors"
	"fmt"
	"reflect"
)

type RowComparator interface {
	Compare(row1, row2 *Row) (*RowDiff, error)
}

type DefaultRowComparator struct {
	IgnoreColumns []string
}

func (cmp *DefaultRowComparator) Compare(row1, row2 *Row) (*RowDiff, error) {
	irow1 := &Row{
		PKCols:       row1.PKCols,
		ColumnValues: make(map[string]ColumnValue),
	}
	irow2 := &Row{
		PKCols:       row1.PKCols,
		ColumnValues: make(map[string]ColumnValue),
	}
	if !reflect.DeepEqual(irow1.PKCols, irow2.PKCols) {
		return nil, errors.New("the primary key of pair of rows must be the same")
	}
	var pk PrimaryKey
	// always includes the primary keys
	for _, pkcn := range irow1.PKCols {
		pk = append(pk, row1.ColumnValues[pkcn])
		irow1.ColumnValues[pkcn] = row1.ColumnValues[pkcn]
		irow2.ColumnValues[pkcn] = row2.ColumnValues[pkcn]
	}
	for cn, cv1 := range row1.ColumnValues {
		ignored := false
		for _, icn := range cmp.IgnoreColumns {
			if icn == cn {
				ignored = true
				break
			}
		}
		if ignored {
			continue
		}

		cv2, exists2 := row2.ColumnValues[cn]
		if !exists2 {
			irow1.ColumnValues[cn] = cv1
			continue
		}
		if !cmp.CompareValues(cv1, cv2) {
			irow1.ColumnValues[cn] = cv1
			irow2.ColumnValues[cn] = cv2
		}
	}
	// check whether column value has diff excluding primary key
	if len(irow1.ColumnValues) <= len(irow1.PKCols) || len(irow2.ColumnValues) <= len(irow2.PKCols) {
		return nil, nil
	}
	return &RowDiff{
		PrimaryKey: pk,
		Row1:       irow1,
		Row2:       irow2,
	}, nil
}

func (cmp *DefaultRowComparator) CompareValues(v1, v2 interface{}) bool {
	return fmt.Sprintf("%s", v1) == fmt.Sprintf("%s", v2)
}

type RowDiff struct {
	PrimaryKey PrimaryKey
	Row1       *Row
	Row2       *Row
}

func CompareRows(rows1, rows2 []*Row, cmp RowComparator) (*RowsDiff, error) {
	rows1Map := rowsToPKMap(rows1)
	rows2Map := rowsToPKMap(rows2)

	diff := &RowsDiff{}
	for pks, row1 := range rows1Map {
		row2, exists2 := rows2Map[pks]
		if !exists2 {
			diff.Rows1Only = append(diff.Rows1Only, row1)
			continue
		}
		rd, err := cmp.Compare(row1, row2)
		if err != nil {
			return nil, err
		}
		if rd != nil {
			diff.DiffRows = append(diff.DiffRows, rd)
		}
	}
	for pks, row2 := range rows2Map {
		if _, exists1 := rows1Map[pks]; !exists1 {
			diff.Rows2Only = append(diff.Rows2Only, row2)
		}
	}
	return diff, nil
}

func rowsToPKMap(rows []*Row) map[string]*Row {
	pkmap := make(map[string]*Row, len(rows))
	for _, row := range rows {
		pks := row.PrimaryKey().String()
		pkmap[pks] = row
	}
	return pkmap
}

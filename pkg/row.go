package pkg

import (
	"fmt"
	"strings"
)

type ColumnValue interface{}

type Row struct {
	PKCols       []string
	ColumnValues map[string]ColumnValue
}

type PrimaryKey []interface{}

func (r *Row) PrimaryKey() PrimaryKey {
	var pk PrimaryKey
	for _, pkcn := range r.PKCols {
		pk = append(pk, r.ColumnValues[pkcn])
	}
	return pk
}

func (pk PrimaryKey) String() string {
	var ks []string
	for _, k := range pk {
		ks = append(ks, fmt.Sprintf("%s", k))
	}
	return strings.Join(ks, "_")
}

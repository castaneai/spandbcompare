package spanner_compare

import (
	"context"
	"fmt"
	"strings"

	"github.com/castaneai/spankeys"

	"cloud.google.com/go/spanner"
)

type ColumnValue interface{}

type Row struct {
	pkColNames   []string
	ColumnValues map[string]ColumnValue
}

func (r *Row) PrimaryKey() PrimaryKey {
	var pk PrimaryKey
	for _, pkcn := range r.pkColNames {
		pk = append(pk, r.ColumnValues[pkcn])
	}
	return pk
}

type PrimaryKey []interface{}

func (pk PrimaryKey) String() string {
	var ks []string
	for _, k := range pk {
		ks = append(ks, fmt.Sprintf("%s", k))
	}
	return strings.Join(ks, "_")
}

type DataSource struct {
	client     *spanner.Client
	table      string
	pkColNames []string
}

func NewDataSource(ctx context.Context, client *spanner.Client, table string) (*DataSource, error) {
	pkCols, err := spankeys.GetPrimaryKeyColumns(ctx, client, table)
	if err != nil {
		return nil, err
	}
	var pkNames []string
	for _, col := range pkCols {
		pkNames = append(pkNames, col.Name)
	}
	return &DataSource{
		client:     client,
		table:      table,
		pkColNames: pkNames,
	}, nil
}

func (s *DataSource) Rows(ctx context.Context) ([]*Row, error) {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT * FROM `%s`", s.table))
	var rows []*Row
	if err := s.client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		row, err := makeRow(r, s.pkColNames)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

func makeRow(r *spanner.Row, pkColNames []string) (*Row, error) {
	row := &Row{
		ColumnValues: make(map[string]ColumnValue),
		pkColNames:   pkColNames,
	}
	for _, cn := range r.ColumnNames() {
		var gcv spanner.GenericColumnValue
		if err := r.ColumnByName(cn, &gcv); err != nil {
			return nil, err
		}
		var cv ColumnValue
		if err := spankeys.DecodeToInterface(&gcv, &cv); err != nil {
			return nil, err
		}
		row.ColumnValues[cn] = cv
	}
	return row, nil
}

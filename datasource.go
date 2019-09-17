package spancompare

import (
	"context"
	"fmt"

	"github.com/castaneai/spankeys"

	"cloud.google.com/go/spanner"
)

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

func makeRow(r *spanner.Row, pkCols []string) (*Row, error) {
	row := &Row{
		ColumnValues: make(map[string]ColumnValue),
		PKCols:       pkCols,
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

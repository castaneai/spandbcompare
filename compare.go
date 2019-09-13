package spanner_compare

import "context"

type Result interface {
}

func Compare(ctx context.Context, ds1, ds2 DataSource) (Result, error) {
	panic("not implemented")
}

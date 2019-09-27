package spandbcompare

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"

	"github.com/castaneai/spankeys/testutils"
)

func TestDataSource_Rows(t *testing.T) {
	ctx := context.Background()
	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := testutils.PrepareDatabase(ctx, []string{
		`CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID)`,
	}); err != nil {
		t.Fatal(err)
	}
	table := "Singers"

	ds, err := NewDataSource(ctx, c, table)
	if err != nil {
		t.Fatal(err)
	}

	{
		rows, err := ds.Rows(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 0, len(rows))
	}

	if _, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		ms := []*spanner.Mutation{
			spanner.Insert(table, []string{"SingerID", "FirstName"}, []interface{}{"singerA", "singerA-name"}),
			spanner.Insert(table, []string{"SingerID", "FirstName"}, []interface{}{"singerB", "singerB-name"}),
		}
		return tx.BufferWrite(ms)
	}); err != nil {
		t.Fatal(err)
	}

	{
		rows, err := ds.Rows(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, "singerA", rows[0].ColumnValues["SingerID"])
		assert.Equal(t, "singerB-name", rows[1].ColumnValues["FirstName"])
	}
}

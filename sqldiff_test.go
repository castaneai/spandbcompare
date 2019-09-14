package spanner_compare

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsertSQL(t *testing.T) {
	pks := []string{"id"}
	ts, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05+09:00")
	if err != nil {
		t.Fatal(err)
	}

	sqls := insertSQL("Singers", []*Row{
		{pks, map[string]ColumnValue{"id": "a", "name": "na", "age": 1, "created_at": ts}},
		{pks, map[string]ColumnValue{"id": "b", "name": "nb", "age": 2, "created_at": ts}},
	})

	assert.Equal(t, 1, len(sqls))
	assert.Equal(t, "INSERT INTO `Singers` (`age`,`created_at`,`id`,`name`) VALUES (1,'2006-01-02T15:04:05+09:00','a','na'),(2,'2006-01-02T15:04:05+09:00','b','nb')", sqls[0])
}

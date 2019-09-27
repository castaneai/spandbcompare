package spandbcompare

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

func TestUpdateSQL(t *testing.T) {
	pks := []string{"ida", "idb"}
	sqls := updateSQL("Singers", []*Row{
		{pks, map[string]ColumnValue{"ida": "aa", "idb": "ab", "age": 10}},
		{pks, map[string]ColumnValue{"ida": "bb", "idb": "bb", "age": 11}},
	})

	assert.Equal(t, 2, len(sqls))
	assert.Equal(t, "UPDATE `Singers` SET `age` = 10 WHERE `ida` = 'aa' and `idb` = 'ab'", sqls[0])
	assert.Equal(t, "UPDATE `Singers` SET `age` = 11 WHERE `ida` = 'bb' and `idb` = 'bb'", sqls[1])
}

func TestDeleteSQL(t *testing.T) {
	pks := []string{"ida", "idb"}
	sqls := deleteSQL("Singers", []*Row{
		{pks, map[string]ColumnValue{"ida": "aa", "idb": "ab", "age": 10}},
		{pks, map[string]ColumnValue{"ida": "bb", "idb": "bb", "age": 11}},
	})

	assert.Equal(t, 2, len(sqls))
	assert.Equal(t, "DELETE FROM `Singers` WHERE `ida` = 'aa' and `idb` = 'ab'", sqls[0])
	assert.Equal(t, "DELETE FROM `Singers` WHERE `ida` = 'bb' and `idb` = 'bb'", sqls[1])
}

func TestSQLDiff_SQL(t *testing.T) {
	pks := []string{"id"}
	rd := &RowsDiff{
		Rows1Only: []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "a-name"}}},
		Rows2Only:[]*Row{{pks, map[string]ColumnValue{"id": "b", "name": "b-name"}}},
		DiffRows: []*RowDiff{{PrimaryKey{"c"},
			&Row{pks, map[string]ColumnValue{"id": "c", "name": "c-name"}},
			&Row{pks, map[string]ColumnValue{"id": "c", "name": "c-name-alt"}},
		}},
	}
	sd, err := NewSQLDiff(rd, "Table1", "Table2")
	if err != nil {
		t.Fatal(err)
	}
	sqls1, err := sd.SQL("Table1")
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range sqls1 {
		t.Logf("%s", sql)
	}

	sqls2, err := sd.SQL("Table2")
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range sqls2 {
		t.Logf("%s", sql)
	}
}
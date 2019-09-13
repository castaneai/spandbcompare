package spanner_compare

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare_NoDiff(t *testing.T) {
	pks := []string{"id"}
	rows1 := []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "a"}}}
	rows2 := []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "a"}}}
	diff, err := Compare(rows1, rows2, &AsStringComparator{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, false, diff.HasDiff())
}

func TestCompare_OnlyDiff(t *testing.T) {
	pks := []string{"id"}

	{
		rows1 := []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "a"}}}
		rows2 := []*Row(nil)
		diff, err := Compare(rows1, rows2, &AsStringComparator{})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, diff.HasDiff())
		assert.Equal(t, 1, len(diff.Rows1Only))
		assert.Equal(t, 0, len(diff.Rows2Only))
		assert.Equal(t, 0, len(diff.Rows1))
		assert.Equal(t, 0, len(diff.Rows2))
	}

	{
		rows1 := []*Row(nil)
		rows2 := []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "a"}}}
		diff, err := Compare(rows1, rows2, &AsStringComparator{})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, diff.HasDiff())
		assert.Equal(t, 0, len(diff.Rows1Only))
		assert.Equal(t, 1, len(diff.Rows2Only))
		assert.Equal(t, 0, len(diff.Rows1))
		assert.Equal(t, 0, len(diff.Rows2))
	}
}

func TestCompare_Diff(t *testing.T) {
	pks := []string{"id"}

	{
		rows1 := []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "na", "age": 1}}}
		rows2 := []*Row{{pks, map[string]ColumnValue{"id": "a", "name": "nb", "age": 1}}}
		diff, err := Compare(rows1, rows2, &AsStringComparator{})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, diff.HasDiff())
		assert.Equal(t, 0, len(diff.Rows1Only))
		assert.Equal(t, 0, len(diff.Rows2Only))
		assert.Equal(t, 1, len(diff.Rows1))
		assert.Equal(t, 1, len(diff.Rows2))
		assert.Equal(t, "a", diff.Rows1[0].ColumnValues["id"])
		assert.Equal(t, "a", diff.Rows2[0].ColumnValues["id"])
		assert.Equal(t, "na", diff.Rows1[0].ColumnValues["name"])
		assert.Equal(t, "nb", diff.Rows2[0].ColumnValues["name"])
		assert.NotContains(t, diff.Rows1[0].ColumnValues, "age")
		assert.NotContains(t, diff.Rows2[0].ColumnValues, "age")
	}
}

func TestCompare_DiffWithCompositePrimaryKeys(t *testing.T) {
	pks := []string{"id1", "id2"}

	{
		rows1 := []*Row{{pks, map[string]ColumnValue{"id1": "A", "id2": "A1", "name": "na"}}}
		rows2 := []*Row{{pks, map[string]ColumnValue{"id1": "A", "id2": "A1", "name": "nb"}}}
		diff, err := Compare(rows1, rows2, &AsStringComparator{})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, diff.HasDiff())
		assert.Equal(t, 0, len(diff.Rows1Only))
		assert.Equal(t, 0, len(diff.Rows2Only))
		assert.Equal(t, 1, len(diff.Rows1))
		assert.Equal(t, 1, len(diff.Rows2))
		assert.Equal(t, "na", diff.Rows1[0].ColumnValues["name"])
		assert.Equal(t, "nb", diff.Rows2[0].ColumnValues["name"])
	}

	{
		rows1 := []*Row{{pks, map[string]ColumnValue{"id1": "A", "id2": "A1", "name": "na"}}}
		rows2 := []*Row{{pks, map[string]ColumnValue{"id1": "A", "id2": "A2", "name": "na"}}}
		diff, err := Compare(rows1, rows2, &AsStringComparator{})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, diff.HasDiff())
		assert.Equal(t, 1, len(diff.Rows1Only))
		assert.Equal(t, 1, len(diff.Rows2Only))
		assert.Equal(t, 0, len(diff.Rows1))
		assert.Equal(t, 0, len(diff.Rows2))
	}
}

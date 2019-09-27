package cli

import (
	"github.com/castaneai/spandbcompare"
	"os"
	"testing"
	"time"
)

func TestDiffAdded(t *testing.T) {
	cols := []string{"id", "name", "age", "created_at"}
	ud, err := NewUnifiedDiff(os.Stderr, cols, "rows1", "rows2")
	if err != nil {
		t.Fatal(err)
	}
	pks := []string{"id"}
	rows := []*spandbcompare.Row{
		{pks, map[string]spandbcompare.ColumnValue{"id": "aaaaaaa", "name": "name-a", "age": 12345, "created_at": time.Now()}},
		{pks, map[string]spandbcompare.ColumnValue{"id": "bbbbbbb", "name": "name-b", "age": nil, "created_at": time.Now()}},
	}
	if err := ud.WriteAdded(rows); err != nil {
		t.Fatal(err)
	}
}

func TestDiffDeleted(t *testing.T) {
	cols := []string{"id", "name", "age", "created_at"}
	ud, err := NewUnifiedDiff(os.Stderr, cols, "rows1", "rows2")
	if err != nil {
		t.Fatal(err)
	}
	pks := []string{"id"}
	rows := []*spandbcompare.Row{
		{pks, map[string]spandbcompare.ColumnValue{"id": "aaaaaaa", "name": "name-a", "age": 12345, "created_at": time.Now()}},
		{pks, map[string]spandbcompare.ColumnValue{"id": "bbbbbbb", "name": "name-b", "age": nil, "created_at": time.Now()}},
	}
	if err := ud.WriteDeleted(rows); err != nil {
		t.Fatal(err)
	}
}

func TestDiffUpdated(t *testing.T) {
	cols := []string{"id1", "id2", "name"}
	ud, err := NewUnifiedDiff(os.Stderr, cols, "rows1", "rows2")
	if err != nil {
		t.Fatal(err)
	}
	pks := []string{"id1", "id2"}
	rows := []*spandbcompare.RowDiff{
		{
			[]interface{}{"a1", "a2"},
			&spandbcompare.Row{pks, map[string]spandbcompare.ColumnValue{"id1": "a1", "id2": "a2", "name": "name-before"}},
			&spandbcompare.Row{pks, map[string]spandbcompare.ColumnValue{"id1": "a1", "id2": "a2", "name": "name-after"}},
		},
	}
	if err := ud.WriteUpdated("before", "after", rows); err != nil {
		t.Fatal(err)
	}
}

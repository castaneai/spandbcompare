package cli

import (
	"os"
	"testing"
	"time"

	"github.com/castaneai/spancompare"
)

func TestDiffAdded(t *testing.T) {
	cols := []string{"id", "name", "age", "created_at"}
	ud, err := NewUnifiedDiff(os.Stderr, cols)
	if err != nil {
		t.Fatal(err)
	}
	pks := []string{"id"}
	rows := []*spancompare.Row{
		{pks, map[string]spancompare.ColumnValue{"id": "aaaaaaa", "name": "name-a", "age": 12345, "created_at": time.Now()}},
		{pks, map[string]spancompare.ColumnValue{"id": "bbbbbbb", "name": "name-b", "age": nil, "created_at": time.Now()}},
	}
	if err := ud.WriteAdded(rows); err != nil {
		t.Fatal(err)
	}
}

func TestDiffDeleted(t *testing.T) {
	cols := []string{"id", "name", "age", "created_at"}
	ud, err := NewUnifiedDiff(os.Stderr, cols)
	if err != nil {
		t.Fatal(err)
	}
	pks := []string{"id"}
	rows := []*spancompare.Row{
		{pks, map[string]spancompare.ColumnValue{"id": "aaaaaaa", "name": "name-a", "age": 12345, "created_at": time.Now()}},
		{pks, map[string]spancompare.ColumnValue{"id": "bbbbbbb", "name": "name-b", "age": nil, "created_at": time.Now()}},
	}
	if err := ud.WriteDeleted(rows); err != nil {
		t.Fatal(err)
	}
}

func TestDiffUpdated(t *testing.T) {
	cols := []string{"id", "name", "age", "created_at"}
	ud, err := NewUnifiedDiff(os.Stderr, cols)
	if err != nil {
		t.Fatal(err)
	}
	pks := []string{"id1", "id2"}
	rows := []*spancompare.RowDiff{
		{
			[]interface{}{"a1", "a2"},
			&spancompare.Row{pks, map[string]spancompare.ColumnValue{"id1": "a1", "id2": "a2", "name": "name-before"}},
			&spancompare.Row{pks, map[string]spancompare.ColumnValue{"id1": "a1", "id2": "a2", "name": "name-after"}},
		},
	}
	if err := ud.WriteUpdated("before", "after", rows); err != nil {
		t.Fatal(err)
	}
}

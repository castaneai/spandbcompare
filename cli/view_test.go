package cli

import (
	"os"
	"testing"
	"time"

	"github.com/castaneai/spancompare"
)

func TestDiffAdded(t *testing.T) {
	da := &DiffAdded{}
	cols := []string{"id", "name", "age", "created_at"}
	pks := []string{"id"}
	rows := []*spancompare.Row{
		{pks, map[string]spancompare.ColumnValue{"id": "aaaaaaa", "name": "name-a", "age": 12345, "created_at": time.Now()}},
		{pks, map[string]spancompare.ColumnValue{"id": "bbbbbbb", "name": "name-b", "age": nil, "created_at": time.Now()}},
	}
	if err := da.Write(os.Stderr, cols, rows); err != nil {
		t.Fatal(err)
	}
}

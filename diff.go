package spancompare

// TablesDiff represents differences between two tables
type TablesDiff struct {
	Table1   string
	Table2   string
	RowsDiff *RowsDiff
}

func (td *TablesDiff) HasDiff() bool {
	return td.RowsDiff != nil && td.RowsDiff.HasDiff()
}

// Differences among rows
// set nil if there is no differences
type RowsDiff struct {
	Rows1Only []*Row
	Rows2Only []*Row
	DiffRows  []*RowDiff
}

func (d *RowsDiff) HasDiff() bool {
	return len(d.Rows1Only) > 0 || len(d.Rows2Only) > 0 || len(d.DiffRows) > 0
}

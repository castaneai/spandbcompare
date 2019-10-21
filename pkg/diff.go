package pkg

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

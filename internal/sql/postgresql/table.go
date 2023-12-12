package postgresql

type Table struct {
	Name        string
	IfNotExists bool
	Columns     []*Column
	Constraints []*Constraint
	Comment     string
}

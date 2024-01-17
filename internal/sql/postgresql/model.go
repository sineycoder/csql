package postgresql

import "github.com/pingcap/tidb/parser/ast"

type Node struct {
	Tables []*Table
}

type Keys []string

type Table struct {
	Name        string
	IfNotExists bool
	Columns     []*Column
	Constraints []*Constraint
	Comment     string
}

type Column struct {
	Name         string
	Type         string
	DefaultValue string
	IsNotNull    bool
	IsPrimaryKey bool
	IsIncrement  bool
	Comment      string
}

type Constraint struct {
	Name  string
	RawTp ast.ConstraintType // ast.ConstraintUniq/ast.ConstraintPrimaryKey can be written in table
	Keys  Keys
}

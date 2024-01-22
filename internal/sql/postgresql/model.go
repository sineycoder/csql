package postgresql

import "github.com/pingcap/tidb/parser/ast"

type Node struct {
	CreateTables []*Table
	AlterTables  []*Table
	Version      float64
}

type Table struct {
	Name        string
	IfNotExists bool
	Columns     []*Column
	Constraints []*Constraint
	Comment     string
}

type Column struct {
	Name           string
	OldName        string // 用于旧名改新名
	Type           string
	AlterTableType ast.AlterTableType // 用于alter table使用，有add、drop、change、modify等
	DefaultValue   string
	IsNotNull      bool
	IsPrimaryKey   bool
	IsIncrement    bool
	Comment        string
}

type Constraint struct {
	Name  string
	RawTp ast.ConstraintType // ast.ConstraintUniq/ast.ConstraintPrimaryKey can be written in table
	Keys  []string
}

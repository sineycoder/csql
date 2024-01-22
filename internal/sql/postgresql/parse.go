package postgresql

import (
	"fmt"

	"github.com/pingcap/tidb/parser/ast"
)

func (v *Node) parseColumn(stmt *ast.CreateTableStmt) []*Column {
	var columns []*Column
	for _, col := range stmt.Cols {
		c := &Column{}
		c.Name = col.Name.Name.O

		// 根据字段类型获取string值
		if c.Type = FieldTypeToString(col.Tp); c.Type == "" {
			panic(fmt.Sprintf("%s.%s' type cannot support in pg", stmt.Table.Name.O, c.Name))
		}

		InjectColumnOption(col, c)

		columns = append(columns, c)
	}

	return columns
}

func (v *Node) parseConstraint(stmt *ast.CreateTableStmt, t *Table) []*Constraint {
	var constraints []*Constraint
	for _, con := range stmt.Constraints {
		c := &Constraint{}
		c.RawTp = con.Tp
		c.Name = con.Name
		for _, key := range con.Keys {
			c.Keys = append(c.Keys, key.Column.Name.O)
		}
		if c.RawTp == ast.ConstraintPrimaryKey {
		j:
			for _, col := range t.Columns {
				for _, key := range c.Keys {
					if col.Name == key {
						col.IsPrimaryKey = true
						break j
					}
				}
			}
		}
		constraints = append(constraints, c)
	}
	return constraints
}

func (v *Node) parseCreateTable(stmt *ast.CreateTableStmt) {
	t := &Table{}
	t.Name = stmt.Table.Name.O
	t.IfNotExists = stmt.IfNotExists

	t.Columns = v.parseColumn(stmt)

	t.Constraints = v.parseConstraint(stmt, t)

	for _, op := range stmt.Options {
		switch op.Tp {
		case ast.TableOptionComment:
			t.Comment = op.StrValue
		}
	}

	v.CreateTables = append(v.CreateTables, t)
}

func (v *Node) parseAlterTable(stmt *ast.AlterTableStmt) {
	fmt.Println(stmt)
	t := &Table{}
	t.Name = stmt.Table.Name.O
	var columns []*Column
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableDropColumn, ast.AlterTableChangeColumn:
			c := &Column{}
			c.AlterTableType = spec.Tp
			if len(spec.NewColumns) > 0 {
				c.Name = spec.NewColumns[0].Name.Name.O
				if c.Type = FieldTypeToString(spec.NewColumns[0].Tp); c.Type == "" {
					panic(fmt.Sprintf("%s.%s' type cannot support in pg", t.Name, c.Name))
				}

				InjectColumnOption(spec.NewColumns[0], c)
			}
			if spec.OldColumnName != nil {
				c.OldName = spec.OldColumnName.Name.O
			}
			columns = append(columns, c)
		default:
			panic("not support such alter table type")
		}
	}
	t.Columns = columns
	fmt.Println(columns)
}

func (v *Node) Enter(in ast.Node) (ast.Node, bool) {
	switch tp := in.(type) {
	case *ast.CreateTableStmt:
		v.parseCreateTable(tp)
		return in, true
	case *ast.AlterTableStmt:
		v.parseAlterTable(tp)
	}
	fmt.Printf("%T -> %+v\n", in, in)
	return in, false
}

func (v *Node) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

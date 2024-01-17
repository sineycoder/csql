package postgresql

import (
	"fmt"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func (v *Node) parseColumn(stmt *ast.CreateTableStmt) []*Column {
	var columns []*Column
	for _, col := range stmt.Cols {
		c := &Column{}
		c.Name = col.Name.Name.O
		switch col.Tp.GetType() {
		case mysql.TypeTiny, mysql.TypeShort:
			c.Type = "smallint"
		case mysql.TypeLong:
			c.Type = "int"
		case mysql.TypeInt24, mysql.TypeLonglong:
			c.Type = "bigint"
		case mysql.TypeFloat:
			c.Type = "real"
		case mysql.TypeDouble:
			c.Type = "double precision"
		case mysql.TypeNewDecimal:
			if col.Tp.GetFlen() == -1 && col.Tp.GetDecimal() == -1 {
				c.Type = "decimal"
			} else {
				c.Type = fmt.Sprintf("decimal(%d,%d)", col.Tp.GetFlen(), col.Tp.GetDecimal())
			}
		case mysql.TypeString:
			c.Type = fmt.Sprintf("char(%d)", col.Tp.GetFlen())
		case mysql.TypeVarchar:
			c.Type = fmt.Sprintf("varchar(%d)", col.Tp.GetFlen())
		case mysql.TypeJSON:
			c.Type = "json"
		case mysql.TypeLongBlob, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob:
			if mysql.HasBinaryFlag(col.Tp.GetFlag()) {
				c.Type = "bytea"
			} else {
				c.Type = "text"
			}
		case mysql.TypeVarString:
			c.Type = "text"
		case mysql.TypeTimestamp, mysql.TypeDatetime:
			c.Type = "timestamp(0)"
		case mysql.TypeDate:
			c.Type = "date"
		default:
			panic(fmt.Sprintf("%s.%s' type cannot support in pg", stmt.Table.Name.O, c.Name))
		}

		for idx, op := range col.Options {
			switch op.Tp {
			case ast.ColumnOptionNotNull:
				c.IsNotNull = true
			case ast.ColumnOptionPrimaryKey:
				c.IsPrimaryKey = true
			case ast.ColumnOptionAutoIncrement:
				c.IsIncrement = true
			case ast.ColumnOptionComment:
				if expr, ok := op.Expr.(*driver.ValueExpr); ok {
					c.Comment = expr.GetDatumString()
				}

			case ast.ColumnOptionDefaultValue:
				switch tp := op.Expr.(type) {
				case *driver.ValueExpr:
					switch tp.Datum.Kind() {
					case types.KindInt64:
						c.DefaultValue = fmt.Sprintf("%d", tp.Datum.GetInt64())
					case types.KindUint64:
						c.DefaultValue = fmt.Sprintf("%d", tp.Datum.GetUint64())
					case types.KindMysqlDecimal:
						d := tp.Datum.GetInterface().(*types.MyDecimal)
						c.DefaultValue = d.String()
					case types.KindString:
						c.DefaultValue = quotaValue(string(tp.Datum.GetBytes()))
					}
				case *ast.FuncCallExpr:
					if col.Tp.GetType() == mysql.TypeTimestamp || col.Tp.GetType() == mysql.TypeDatetime {
						c.DefaultValue = tp.FnName.O
					} else {
						panic(fmt.Sprintf("%s.%s %d option cannot support in pg", stmt.Table.Name.O, c.Name, idx))
					}
				}
			}
		}
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

func (v *Node) parseTable(stmt *ast.CreateTableStmt) {
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

	v.Tables = append(v.Tables, t)
}

func (v *Node) Enter(in ast.Node) (ast.Node, bool) {
	if sc, ok := in.(*ast.CreateTableStmt); ok {
		v.parseTable(sc)
		return in, true
	}
	fmt.Printf("%T -> %+v\n", in, in)
	return in, false
}

func (v *Node) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

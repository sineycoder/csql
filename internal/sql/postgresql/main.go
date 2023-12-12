package postgresql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/csql/internal/sql"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type Node struct {
	Tables []*Table
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

type Keys []string

type Constraint struct {
	Name  string
	RawTp ast.ConstraintType // ast.ConstraintUniq/ast.ConstraintPrimaryKey can be written in table
	Keys  Keys
}

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
	//fmt.Printf("%T -> %+v\n", in, in)
	return in, false
}

func (v *Node) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (c *Column) Write(buf *sql.SQLBuffer) (isWrite bool) {
	buf.WriteByte('\t')
	buf.WriteString(quota(c.Name))
	buf.WriteByte('\t')
	buf.WriteString(c.Type)
	if c.IsNotNull {
		buf.WriteByte('\t')
		buf.WriteString("NOT NULL")
	}
	if c.DefaultValue != "" {
		buf.WriteByte('\t')
		buf.WriteString("DEFAULT ")
		buf.WriteString(c.DefaultValue)
	}
	buf.WriteByte(',')
	return true
}

func (c *Constraint) Write(buf *sql.SQLBuffer) (isWrite bool) {
	fn := func(keys []string) {
		for idx, key := range keys {
			if idx > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(quota(key))
		}
	}
	switch c.RawTp {
	case ast.ConstraintPrimaryKey:
		buf.WriteString("\tPRIMARY KEY ")
	case ast.ConstraintUniq:
		buf.WriteString("\tCONSTRAINT ")
		buf.WriteString(c.Name)
		buf.WriteString(" UNIQUE ")
	default:
		return
	}
	buf.WriteByte('(')
	fn(c.Keys)
	buf.WriteByte(')')
	buf.WriteByte(',')
	return true
}

func (n *Node) ToSQL() string {
	resBuf := sql.NewSQLBuffer()
	tmpBuf := sql.NewSQLBuffer()

	// ddl
	hasAutoIncrement := false
	hasIndex := false
	for _, t := range n.Tables {
		// table
		tmpBuf.Reset()
		tmpBuf.WriteString("CREATE TABLE ")
		if t.IfNotExists {
			tmpBuf.WriteString("IF NOT EXISTS ")
		}
		tmpBuf.WriteString(quota(t.Name))
		tmpBuf.WriteByte('(')
		tmpBuf.WriteByte('\n')

		// colum
		for _, column := range t.Columns {
			if column.Write(tmpBuf) {
				tmpBuf.WriteByte('\n')
			}
			hasAutoIncrement = hasAutoIncrement || column.IsPrimaryKey && column.IsIncrement
		}

		// index
		for _, constraint := range t.Constraints {
			if constraint.Write(tmpBuf) {
				tmpBuf.WriteByte('\n')
			}
			hasIndex = hasIndex || constraint.RawTp == ast.ConstraintIndex
		}
		bs := tmpBuf.Bytes()
		if len(bs)-2 == bytes.LastIndexByte(bs, ',') {
			tmpBuf.Truncate(tmpBuf.Len() - 2)
			tmpBuf.WriteByte('\n')
		}
		tmpBuf.WriteStringln(");")

		// 表注释

		if len(t.Comment) > 0 {
			tmpBuf.WriteStringln(fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", t.Name, t.Comment))
		}

		// 列注释
		for _, col := range t.Columns {
			if len(col.Comment) > 0 {
				tmpBuf.WriteStringln(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'", t.Name, col.Name, col.Comment))
			}
		}

		tmpBuf.WriteByte('\n')
		resBuf.Write(tmpBuf.Bytes())
	}

	if hasAutoIncrement {
		resBuf.WriteStringln("DO")
		resBuf.WriteStringln("$BLOCK$")
		resBuf.WriteNTabStringln("BEGIN", 1)
		for _, t := range n.Tables {
			for _, col := range t.Columns {
				if col.IsPrimaryKey && col.IsIncrement {
					resBuf.WriteNTabStringln("BEGIN", 2)
					resBuf.WriteNTabStringln(fmt.Sprintf(`CREATE SEQUENCE "%s_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1;`, t.Name), 3)
					resBuf.WriteNTabStringln(fmt.Sprintf(`ALTER SEQUENCE "%s_id_seq" OWNED BY "%s".%s;`, t.Name, t.Name, col.Name), 3)
					resBuf.WriteNTabStringln(fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN id SET DEFAULT nextval('%s_id_seq');`, t.Name, t.Name), 3)
					resBuf.WriteNTabStringln("EXCEPTION", 2)
					resBuf.WriteNTabStringln("WHEN OTHERS", 3)
					resBuf.WriteNTabStringln(fmt.Sprintf(`THEN RAISE NOTICE 'create %s_id_seq sequence err';`, t.Name), 4)
					resBuf.WriteNTabStringln("END;", 2)

				}
			}
		}
		resBuf.WriteNTabStringln("END;", 1)
		resBuf.WriteStringln("$BLOCK$;")
		resBuf.WriteStringln("END;")
		resBuf.WriteByte('\n')
	}

	if hasIndex {
		resBuf.WriteStringln("DO")
		resBuf.WriteStringln("$BLOCK$")
		resBuf.WriteNTabStringln("BEGIN", 1)
		for _, t := range n.Tables {
			for _, con := range t.Constraints {
				if con.RawTp == ast.ConstraintIndex {
					resBuf.WriteNTabStringln("BEGIN", 2)
					indexName := "idx_" + t.Name
					var keyName []string
					for _, k := range con.Keys {
						indexName += "_" + k
						keyName = append(keyName, quota(k))
					}
					resBuf.WriteNTabStringln(fmt.Sprintf(`CREATE INDEX "%s" ON "%s" (%s);`, indexName, t.Name, strings.Join(keyName, ",")), 3)
					resBuf.WriteNTabStringln("EXCEPTION", 2)
					resBuf.WriteNTabStringln("WHEN duplicate_table", 3)
					resBuf.WriteNTabStringln(fmt.Sprintf(`THEN RAISE NOTICE 'index ''%s'' on %s already exists, skipping';`, indexName, t.Name), 4)
					resBuf.WriteNTabStringln("END;", 2)
				}
			}
		}
		resBuf.WriteNTabStringln("END;", 1)
		resBuf.WriteStringln("$BLOCK$;")
		resBuf.WriteStringln("END;")
		resBuf.WriteByte('\n')
	}

	return strings.TrimSpace(resBuf.String())
}

func quota(fieldName string) string {
	return "\"" + fieldName + "\""
}

func quotaValue(value string) string {
	return "'" + value + "'"
}

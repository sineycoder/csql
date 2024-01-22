package postgresql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/sineycoder/csql/sql"
)

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

func (n *Node) writeSQLWithCreateTable(buf *sql.SQLBuffer) {
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
			tmpBuf.WriteStringln(fmt.Sprintf("COMMENT ON TABLE %s IS '%s';", t.Name, t.Comment))
		}

		// 列注释
		for _, col := range t.Columns {
			if len(col.Comment) > 0 {
				tmpBuf.WriteStringln(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", t.Name, col.Name, col.Comment))
			}
		}

		tmpBuf.WriteByte('\n')
		buf.Write(tmpBuf.Bytes())
	}

	if hasAutoIncrement {
		buf.WriteStringln("DO")
		buf.WriteStringln("$BLOCK$")
		buf.WriteNTabStringln("BEGIN", 1)
		for _, t := range n.Tables {
			for _, col := range t.Columns {
				if col.IsPrimaryKey && col.IsIncrement {
					buf.WriteNTabStringln("BEGIN", 2)
					buf.WriteNTabStringln(fmt.Sprintf(`CREATE SEQUENCE "%s_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1;`, t.Name), 3)
					buf.WriteNTabStringln(fmt.Sprintf(`ALTER SEQUENCE "%s_id_seq" OWNED BY "%s".%s;`, t.Name, t.Name, col.Name), 3)
					buf.WriteNTabStringln(fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN id SET DEFAULT nextval('%s_id_seq');`, t.Name, t.Name), 3)
					buf.WriteNTabStringln("EXCEPTION", 2)
					buf.WriteNTabStringln("WHEN OTHERS", 3)
					buf.WriteNTabStringln(fmt.Sprintf(`THEN RAISE NOTICE 'create %s_id_seq sequence err';`, t.Name), 4)
					buf.WriteNTabStringln("END;", 2)

				}
			}
		}
		buf.WriteNTabStringln("END;", 1)
		buf.WriteStringln("$BLOCK$;")
		buf.WriteStringln("END;")
		buf.WriteByte('\n')
	}

	if hasIndex {
		if n.Version >= 9.5 {
			for _, t := range n.Tables {
				for _, con := range t.Constraints {
					if con.RawTp == ast.ConstraintIndex {
						indexName := "idx_" + t.Name
						var keyName []string
						for _, k := range con.Keys {
							indexName += "_" + k
							keyName = append(keyName, quota(k))
						}
						buf.WriteStringln(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" ON "%s" (%s);`, indexName, t.Name, strings.Join(keyName, ",")))
					}
				}
			}
		} else {
			buf.WriteStringln("DO")
			buf.WriteStringln("$BLOCK$")
			buf.WriteNTabStringln("BEGIN", 1)
			for _, t := range n.Tables {
				for _, con := range t.Constraints {
					if con.RawTp == ast.ConstraintIndex {
						buf.WriteNTabStringln("BEGIN", 2)
						indexName := "idx_" + t.Name
						var keyName []string
						for _, k := range con.Keys {
							indexName += "_" + k
							keyName = append(keyName, quota(k))
						}
						buf.WriteNTabStringln(fmt.Sprintf(`CREATE INDEX "%s" ON "%s" (%s);`, indexName, t.Name, strings.Join(keyName, ",")), 3)
						buf.WriteNTabStringln("EXCEPTION", 2)
						buf.WriteNTabStringln("WHEN duplicate_table", 3)
						buf.WriteNTabStringln(fmt.Sprintf(`THEN RAISE NOTICE 'index ''%s'' on %s already exists, skipping';`, indexName, t.Name), 4)
						buf.WriteNTabStringln("END;", 2)
					}
				}
			}
			buf.WriteNTabStringln("END;", 1)
			buf.WriteStringln("$BLOCK$;")
			buf.WriteStringln("END;")
		}
		buf.WriteByte('\n')
	}
}

func (n *Node) ToSQL() string {
	buf := sql.NewSQLBuffer()
	n.writeSQLWithCreateTable(buf)
	return strings.TrimSpace(buf.String())
}

func quota(fieldName string) string {
	return "\"" + fieldName + "\""
}

func quotaValue(value string) string {
	return "'" + value + "'"
}

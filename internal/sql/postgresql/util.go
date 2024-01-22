package postgresql

import (
	"fmt"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func InjectColumnOption(col *ast.ColumnDef, c *Column) {
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
					panic(fmt.Sprintf("%s %d option cannot support in pg", c.Name, idx))
				}
			}
		}
	}
}

func FieldTypeToString(tp *types.FieldType) string {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort:
		return "smallint"
	case mysql.TypeLong:
		return "int"
	case mysql.TypeInt24, mysql.TypeLonglong:
		return "bigint"
	case mysql.TypeFloat:
		return "real"
	case mysql.TypeDouble:
		return "double precision"
	case mysql.TypeNewDecimal:
		if tp.GetFlen() == -1 && tp.GetDecimal() == -1 {
			return "decimal"
		} else {
			return fmt.Sprintf("decimal(%d,%d)", tp.GetFlen(), tp.GetDecimal())
		}
	case mysql.TypeString:
		return fmt.Sprintf("char(%d)", tp.GetFlen())
	case mysql.TypeVarchar:
		return fmt.Sprintf("varchar(%d)", tp.GetFlen())
	case mysql.TypeJSON:
		return "json"
	case mysql.TypeLongBlob, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob:
		if mysql.HasBinaryFlag(tp.GetFlag()) {
			return "bytea"
		} else {
			return "text"
		}
	case mysql.TypeVarString:
		return "text"
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return "timestamp(0)"
	case mysql.TypeDate:
		return "date"
	default:
		return ""
	}
}

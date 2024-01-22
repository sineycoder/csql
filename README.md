# CSQL
Convert mysql db ddl to other db ddl.

# Support
Only support following db type:
* postgresql
* postgresql-compatible (< postgresql 9.4)
* oracle(not support current)

# Usage

* cmd
```bash
# build csql
go build -o csql

csql -o <filepath> <sqlfile>

# select db type that you want to convert.
? Select db type you want to convert to: 
  ▸ postgresql-compatible
    postgresql
# result will be saved to <filepath>

# TODO: print result if without -o
```

* import
```go
import (
    "github.com/sineycoder/csql/sql/mysql"
)

func main() {
    res, err := mysql.ParseSQLToString("your mysql sql", mysql.PostgresqlCompatible)
    if err != nil {
        panic(err)
    }
}
```

# Example
```sql
CREATE TABLE `test` (
  `id` int(11) NOT NULL
```
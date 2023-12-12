package sql

type Table interface {
	ToSQL() string
}

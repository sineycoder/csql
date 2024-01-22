package mysql

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/csql/internal/sql/postgresql"
	"github.com/manifoldco/promptui"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type DBType string

const (
	PostgresqlCompatible DBType = "postgresql-compatible"
	Postgresql           DBType = "postgresql"
	Oracle               DBType = "oracle"
)

var (
	SupportDBTypes = []DBType{PostgresqlCompatible, Postgresql}
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	if stmtNodes == nil {
		return nil, nil
	}
	return &stmtNodes[0], nil
}

var (
	reg1 = regexp.MustCompile("/\\*[\\s\\S]*\\*/")
	reg2 = regexp.MustCompile("--.*\n")
)

func ParseSQLToString(sql string, dbType DBType) (string, error) {
	splits := strings.Split(sql, ";")
	tableNode := &postgresql.Node{}
	for _, s := range splits {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		node, err := parse(s + ";")
		if err != nil {
			return "", err
		}
		if node != nil {
			(*node).Accept(tableNode)
		}
	}

	var result string
	switch dbType {
	case PostgresqlCompatible:
		tableNode.Version = 9.4
		return tableNode.ToSQL(), nil
	case Postgresql:
		tableNode.Version = 9.5
		return tableNode.ToSQL(), nil
	}
	return result, fmt.Errorf("not support %s", dbType)
}

func Run(path string, output string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("seleted path must be a mysql sql file")
	}

	sql := string(b)
	sql = reg1.ReplaceAllString(sql, "")
	sql = reg2.ReplaceAllString(sql, "")
	sql = strings.TrimSpace(sql)

	prompt := promptui.Select{
		Label: "Select db type you want to convert to",
		Items: SupportDBTypes,
	}

	_, choose, err := prompt.Run()
	if err != nil {
		return err
	}

	result, err := ParseSQLToString(sql, DBType(choose))
	if err != nil {
		return err
	}

	if output != "" {
		return os.WriteFile(output, []byte(result), os.ModePerm)
	}
	fmt.Println(result)
	return nil
}

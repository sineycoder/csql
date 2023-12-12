package mysql

import (
	"fmt"
	"os"
	"strings"

	"github.com/csql/internal/sql/postgresql"
	"github.com/manifoldco/promptui"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

const (
	Postgresql = "postgresql"
	Oracle     = "oracle"
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

func Run(path string, output string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("seleted path must be a mysql sql file")
	}

	prompt := promptui.Select{
		Label: "Select db type you want to convert to",
		Items: []string{Postgresql},
	}

	_, choose, err := prompt.Run()
	if err != nil {
		return err
	}

	splits := strings.Split(string(b), ";")
	tableNode := &postgresql.Node{}
	for _, s := range splits {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		node, err := parse(s + ";")
		if err != nil {
			return err
		}
		if node != nil {
			(*node).Accept(tableNode)
		}
	}

	var result string
	switch choose {
	case Postgresql:
		result = tableNode.ToSQL()
	default:
		return fmt.Errorf("not support")
	}
	if output != "" {
		return os.WriteFile(output, []byte(result), os.ModePerm)
	}
	fmt.Println(result)
	return nil
}

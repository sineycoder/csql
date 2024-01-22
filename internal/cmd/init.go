package cmd

import (
	"fmt"
	"os"

	"github.com/sineycoder/csql/internal/sql/mysql"
	"github.com/urfave/cli/v2"
)

func Init() error {
	app := &cli.App{
		Name:        "csql command tool",
		Usage:       "csql {your mysql path}",
		UsageText:   "csql {your mysql path}",
		Description: "convert mysql grammar to postgresql/oracle/...",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
			},
		},
		Action: func(ctx *cli.Context) error {
			result := ctx.Args().Slice()
			if len(result) == 0 {
				return fmt.Errorf("mysql sql path is required")
			}
			output := ctx.String("output")
			return mysql.Run(result[0], output)
		},
	}
	return app.Run(os.Args)
}

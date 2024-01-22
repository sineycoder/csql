package main

import "github.com/sineycoder/csql/internal/cmd"

func main() {
	if err := cmd.Init(); err != nil {
		panic(err)
	}
}

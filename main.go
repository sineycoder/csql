package main

import "github.com/csql/internal/cmd"

func main() {
	if err := cmd.Init(); err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"os"

	"go.vitalvas.com/mqtt-forward/internal/cmd"
	"go.vitalvas.com/mqtt-forward/internal/system"
)

var version string

func main() {
	system.InitResolver()
	cmd.SetVersion(version)

	root, err := cmd.NewRootCmd()
	if err != nil {
		os.Exit(1)
	}

	if err := root.Execute(context.Background()); err != nil {
		os.Exit(1)
	}
}

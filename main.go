package main

import (
	"os"

	"go.vitalvas.com/mqtt-forward/internal/cmd"
	"go.vitalvas.com/mqtt-forward/internal/system"
)

var version string

func main() {
	system.InitResolver()
	cmd.SetVersion(version)

	if err := cmd.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

package main

import (
	"os"

	"github.com/vitalvas/mqtt-forward/internal/cmd"
	"github.com/vitalvas/mqtt-forward/internal/system"
)

var version string

func main() {
	system.InitResolver()
	cmd.SetVersion(version)

	if err := cmd.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

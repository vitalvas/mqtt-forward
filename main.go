package main

import (
	"os"

	"github.com/vitalvas/mqtt-forward/internal/cmd"
)

var version string

func main() {
	cmd.SetVersion(version)

	if err := cmd.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

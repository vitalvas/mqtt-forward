package main

import (
	"os"

	"github.com/vitalvas/mqtt-forward/internal/cmd"
)

func main() {
	if err := cmd.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

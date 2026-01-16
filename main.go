package main

import (
	"Redis_Go/config"
	"Redis_Go/lib/logger"
	"Redis_Go/tcp"
	EchoHandler "Redis_Go/tcp"
	"fmt"
	"os"
)

const configFile string = "redis.conf"

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "Redis_Go",
		Ext:        "log",
		TimeFormat: "2026-01-17",
	})

	if fileExists(configFile) {
		config.SetupConfig(configFile)
	}

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d",
				config.Properties.Bind,
				config.Properties.Port),
		},
		EchoHandler.GetHandler())

	if err != nil {
		logger.Error(err)
	}
}

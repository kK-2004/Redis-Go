package main

import (
	"Redis_Go/config"
	"Redis_Go/lib/logger"
	"Redis_Go/resp/handler"
	"Redis_Go/tcp"
	"fmt"
	"os"
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	// 获取配置文件路径，默认为 redis.conf
	configFile := "redis.conf"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	fmt.Printf("Loading config file: %s\n", configFile)

	if !fileExists(configFile) {
		fmt.Printf("Config file not found: %s\n", configFile)
		os.Exit(1)
	}
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "Redis_Go",
		Ext:        "log",
		TimeFormat: "2006-01-02",
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
		handler.GetHandler())

	if err != nil {
		logger.Error(err)
	}
}

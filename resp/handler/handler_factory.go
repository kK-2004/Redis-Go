package handler

import (
	"Redis_Go/interface/tcp"
	"Redis_Go/lib/logger"
)

func GetHandler(strategy ...string) tcp.Handler {
	if strategy == nil || strategy[0] == "" || strategy[0] == "resp" {
		return GetRespHandler()
	} else if strategy[0] == "echo" {
		return GetEchoHandler()
	} else {
		logger.Error("unknown strategy: " + strategy[0])
	}
	return nil
}

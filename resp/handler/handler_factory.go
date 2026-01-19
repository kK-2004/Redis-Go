package handler

import (
	"Redis_Go/config/handler"
	"Redis_Go/interface/tcp"
	"Redis_Go/lib/logger"
)

func GetHandler(opts ...handler.Option) tcp.Handler {
	cfg := &handler.Conf{
		Strategy: "resp",
		Use_db:   "db",
	}
	for _, opt := range opts {
		opt(cfg)
	}
	switch cfg.Strategy {
	case "resp":
		switch cfg.Use_db {
		case "db":
			logger.Info("[handler]: resp [db]: ", cfg.Use_db)
			return GetRespHandler(cfg.Use_db)
		default:
			logger.Info("[handler]: resp [db]: ", cfg.Use_db)
			return GetRespHandler(cfg.Use_db)
		}
	default:
		logger.Info("[handler]: echo ")
		return GetEchoHandler()
	}
}

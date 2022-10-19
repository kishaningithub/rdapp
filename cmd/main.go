package main

import (
	"flag"
	"fmt"
	"github.com/kishaningithub/rdapp/pkg"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

const examples = `
examples:
	rdapp`

func main() {
	var options rdapp.Options
	logger := constructLogger()
	defer logger.Sync()
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: rdapp [options]")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, examples)
	}
	flag.StringVar(&options.ListenAddress, "listen", "127.0.0.1:25432", "Listen address")
	flag.Parse()

	err := rdapp.NewPostgresRedshiftDataAPIProxy(options, logger).Run()
	if err != nil {
		logger.Error("error while creating postgres redshift proxy", zap.Error(err))
	}
}

func constructLogger() *zap.Logger {
	productionConfig := zap.NewProductionConfig()
	productionConfig.EncoderConfig.TimeKey = "timestamp"
	productionConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	productionConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger, _ := productionConfig.Build()
	return logger
}

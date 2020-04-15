package logger

import (
	"fmt"
	"sync"

	"github.com/zhiqiangxu/redisbed/pkg/config"
	l "github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger *zap.Logger
	mu     sync.Mutex
)

// Instance is singleton for zap.Logger
func Instance() *zap.Logger {
	if logger != nil {
		return logger
	}

	mu.Lock()
	defer mu.Unlock()
	if logger != nil {
		return logger
	}

	conf := config.Load()
	var lvl zap.AtomicLevel
	err := lvl.UnmarshalText([]byte(conf.LogLevel))
	if err != nil {
		panic(fmt.Sprintf("UnmarshalText:%v", err))
	}

	var encoderConfig zapcore.EncoderConfig
	if conf.Prod {
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	} else {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
	}

	zconf := zap.Config{
		DisableCaller:     true,
		DisableStacktrace: true,
		Level:             lvl,
		Development:       !conf.Prod,
		Encoding:          "json",
		EncoderConfig:     encoderConfig,
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stderr"},
	}

	logger, err = l.New(zconf)
	if err != nil {
		panic(fmt.Sprintf("Build:%v", err))
	}

	return logger
}

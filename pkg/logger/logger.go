package logger

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Log = zap.NewNop()

func Initialize(level string) error {
	lvl, err := zap.ParseAtomicLevel(level)
	if err != nil {
		return fmt.Errorf("log parse level: %w", err)
	}

	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)

	cfg.Level = lvl

	zl, err := cfg.Build()
	if err != nil {
		return fmt.Errorf("log build: %w", err)
	}

	Log = zl
	return nil
}

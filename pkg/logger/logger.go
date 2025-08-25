// pkg/logger/logger.go
package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewDevelopment creates a new logger instance optimized for development.
// It provides human-readable, colored output at the Debug level.
func NewDevelopment() (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return config.Build()
}

// NewProduction creates a new logger instance optimized for production.
// It provides structured, JSON-formatted output at the Info level.
func NewProduction() (*zap.Logger, error) {
	return zap.NewProduction()
}
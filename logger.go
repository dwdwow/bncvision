package bncvision

import (
	"log/slog"
	"os"
)

// gLogger is the global logger instance using slog
var gLogger *slog.Logger

func init() {
	// Initialize the logger with JSON handler
	gLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Set the default log level
	}).WithAttrs([]slog.Attr{
		slog.String("package", "bncvision"),
	}))
}

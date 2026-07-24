package logger

import "go.uber.org/zap"

// logger.go sets up a structured JSON logger (like Zap) for the application.
// It ensures production logs are readable and easy to query in tools like Datadog or Kibana.

// Log is a global variable holding our Zap logger instance
var Log *zap.Logger

func InitLogger() error {
	var err error
	// NewProduction gives us high-performance, JSON-formatted logs
	Log, err = zap.NewProduction()
	if err != nil {
		return err
	}
	return nil
}

// Sync flushes any buffered log entries (we will defer this in main.go)
func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}

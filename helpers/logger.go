package helpers

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

// Define log levels
const (
	DEBUG = iota
	INFO
	WARN
	ERROR
)

// Set the current log level;
// messages with a level lower than this will not be logged.
var currentLogLevel = DEBUG

// CustomLogger wraps the standard logger.
type CustomLogger struct {
	logger *log.Logger
	level  int
}

// NewCustomLogger creates a new CustomLogger.
// add date and time and filename and line number
func NewCustomLogger(prefix string, level int) *CustomLogger {
	return &CustomLogger{
		logger: log.New(os.Stdout, prefix, log.Ldate|log.Ltime),
		level:  level,
	}
}

// Log a message at a given level if it meets the current log level threshold.
func (l *CustomLogger) Log(level int, format string, args ...interface{}) {
	if level >= currentLogLevel {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			format = fmt.Sprintf("%s:%d: %s", filepath.Base(file), line, format)
		}
		l.logger.Printf(format, args...)
	}
}

/*
	debugLogger := NewCustomLogger("DEBUG: ", DEBUG)
	DebugLogger := NewCustomLogger("INFO: ", INFO)
	warnLogger := NewCustomLogger("WARN: ", WARN)

	debugLogger.Log(DEBUG, "This is a debug message")
	DebugLogger.Log(INFO, "This is an info message")
	warnLogger.Log(WARN, "This is a warning message")
	errorLogger.Log(ERROR, "This is an error message")
*/

var DebugLogger = NewCustomLogger("DEBUG: ", DEBUG)
var InfoLogger = NewCustomLogger("INFO: ", INFO)
var WarnLogger = NewCustomLogger("WARN: ", WARN)
var ErrorLogger = NewCustomLogger("ERROR: ", ERROR)

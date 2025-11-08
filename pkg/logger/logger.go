package logger

import (
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type loggerKey struct{}

var globalLogger zerolog.Logger

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	pname, err := os.Executable()
	if err != nil {
		panic(err)
	}

	level := zerolog.InfoLevel
	level, err = zerolog.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil || level == zerolog.NoLevel {
		level = zerolog.InfoLevel
		log.Warn().Err(err).Msgf("invalid LOG_LEVEL, defaulting to INFO")
	}

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	globalLogger = log.With().
		Str("hostname", hostname).
		Str("executable", filepath.Base(pname)).
		Stack().
		Caller().
		Logger().
		Level(level)

	log.Logger = globalLogger
}

func Ctx(ctx context.Context) *zerolog.Logger {
	if ctx == nil {
		return &globalLogger
	}
	return ctx.Value(loggerKey{}).(*zerolog.Logger)
}

func WithLogger(ctx context.Context, logger *zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// SetLevel updates the global log level
func SetLevel(level zerolog.Level) {
	globalLogger = globalLogger.Level(level)
	log.Logger = globalLogger
}

// Fatal logs a fatal message and exits
func Fatal() *zerolog.Event {
	return globalLogger.Fatal()
}

// Error logs an error message
func Error() *zerolog.Event {
	return globalLogger.Error()
}

// Warn logs a warning message
func Warn() *zerolog.Event {
	return globalLogger.Warn()
}

// Info logs an info message
func Info() *zerolog.Event {
	return globalLogger.Info()
}

// Debug logs a debug message
func Debug() *zerolog.Event {
	return globalLogger.Debug()
}

// Trace logs a trace message
func Trace() *zerolog.Event {
	return globalLogger.Trace()
}

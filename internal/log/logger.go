package log

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 日志接口
type Logger interface {
	Debug(msg string, fields ...interface{})
	Debugf(format string, args ...interface{})
	Info(msg string, fields ...interface{})
	Infof(format string, args ...interface{})
	Warn(msg string, fields ...interface{})
	Warnf(format string, args ...interface{})
	Error(msg string, fields ...interface{})
	Errorf(format string, args ...interface{})
	With(fields ...interface{}) Logger
	Sync() error
}

// ZapLogger zap 日志包装器
type ZapLogger struct {
	sugar *zap.SugaredLogger
}

// New 创建日志实例
func New(level string) Logger {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "time"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// 解析日志级别
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}
	cfg.Level = zap.NewAtomicLevelAt(zapLevel)

	logger, err := cfg.Build(zap.AddCallerSkip(1))
	if err != nil {
		// fallback to nop logger
		return &ZapLogger{sugar: zap.NewNop().Sugar()}
	}

	return &ZapLogger{sugar: logger.Sugar()}
}

// NewDevelopment 创建开发模式日志
func NewDevelopment() Logger {
	logger, err := zap.NewDevelopment(zap.AddCallerSkip(1))
	if err != nil {
		return &ZapLogger{sugar: zap.NewNop().Sugar()}
	}
	return &ZapLogger{sugar: logger.Sugar()}
}

// NewNop 创建空日志（用于测试）
func NewNop() Logger {
	return &ZapLogger{sugar: zap.NewNop().Sugar()}
}

// Debug 输出 debug 日志
func (l *ZapLogger) Debug(msg string, fields ...interface{}) {
	l.sugar.Debugw(msg, fields...)
}

// Debugf 格式化输出 debug 日志
func (l *ZapLogger) Debugf(format string, args ...interface{}) {
	l.sugar.Debugf(format, args...)
}

// Info 输出 info 日志
func (l *ZapLogger) Info(msg string, fields ...interface{}) {
	l.sugar.Infow(msg, fields...)
}

// Infof 格式化输出 info 日志
func (l *ZapLogger) Infof(format string, args ...interface{}) {
	l.sugar.Infof(format, args...)
}

// Warn 输出 warn 日志
func (l *ZapLogger) Warn(msg string, fields ...interface{}) {
	l.sugar.Warnw(msg, fields...)
}

// Warnf 格式化输出 warn 日志
func (l *ZapLogger) Warnf(format string, args ...interface{}) {
	l.sugar.Warnf(format, args...)
}

// Error 输出 error 日志
func (l *ZapLogger) Error(msg string, fields ...interface{}) {
	l.sugar.Errorw(msg, fields...)
}

// Errorf 格式化输出 error 日志
func (l *ZapLogger) Errorf(format string, args ...interface{}) {
	l.sugar.Errorf(format, args...)
}

// With 创建带有额外字段的日志实例
func (l *ZapLogger) With(fields ...interface{}) Logger {
	return &ZapLogger{sugar: l.sugar.With(fields...)}
}

// Sync 同步日志
func (l *ZapLogger) Sync() error {
	return l.sugar.Sync()
}

// 全局默认日志实例
var defaultLogger Logger = New("info")

// SetDefault 设置默认日志实例
func SetDefault(l Logger) {
	defaultLogger = l
}

// Default 获取默认日志实例
func Default() Logger {
	return defaultLogger
}

// 全局便捷函数
func Debug(msg string, fields ...interface{}) { defaultLogger.Debug(msg, fields...) }
func Debugf(format string, args ...interface{}) { defaultLogger.Debugf(format, args...) }
func Info(msg string, fields ...interface{})  { defaultLogger.Info(msg, fields...) }
func Infof(format string, args ...interface{})  { defaultLogger.Infof(format, args...) }
func Warn(msg string, fields ...interface{})  { defaultLogger.Warn(msg, fields...) }
func Warnf(format string, args ...interface{})  { defaultLogger.Warnf(format, args...) }
func Error(msg string, fields ...interface{}) { defaultLogger.Error(msg, fields...) }
func Errorf(format string, args ...interface{}) { defaultLogger.Errorf(format, args...) }

// 兼容 fmt 包的便捷函数
func Printf(format string, args ...interface{}) {
	defaultLogger.Info(fmt.Sprintf(format, args...))
}

func Println(args ...interface{}) {
	defaultLogger.Info(fmt.Sprint(args...))
}

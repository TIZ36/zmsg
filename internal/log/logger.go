package log

import (
	"fmt"
	"os"
	"strings"

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

// Config 日志配置
type Config struct {
	// Level 日志级别: debug, info, warn, error
	Level string `yaml:"level" json:"level"`

	// Encoding 输出格式: json, console
	Encoding string `yaml:"encoding" json:"encoding"`

	// AddCaller 是否添加调用者信息（文件路径和行号）
	AddCaller bool `yaml:"add_caller" json:"add_caller"`

	// CallerSkip 调用栈跳过层数（默认1）
	CallerSkip int `yaml:"caller_skip" json:"caller_skip"`

	// Development 开发模式（更易读的输出，打印堆栈跟踪）
	Development bool `yaml:"development" json:"development"`

	// DisableStacktrace 禁用堆栈跟踪
	DisableStacktrace bool `yaml:"disable_stacktrace" json:"disable_stacktrace"`

	// StacktraceLevel 启用堆栈跟踪的日志级别: warn, error, panic
	StacktraceLevel string `yaml:"stacktrace_level" json:"stacktrace_level"`

	// OutputPaths 输出路径，支持 stdout, stderr, 或文件路径
	OutputPaths []string `yaml:"output_paths" json:"output_paths"`

	// ErrorOutputPaths 错误输出路径
	ErrorOutputPaths []string `yaml:"error_output_paths" json:"error_output_paths"`

	// TimeFormat 时间格式: iso8601, epoch, millis, nanos
	TimeFormat string `yaml:"time_format" json:"time_format"`

	// ColorOutput 彩色输出（仅 console 模式有效）
	ColorOutput bool `yaml:"color_output" json:"color_output"`

	// MetricsEnabled 是否启用 metrics（保留字段，用于兼容）
	MetricsEnabled bool `yaml:"metrics_enabled" json:"metrics_enabled"`
}

// DefaultConfig 默认配置
func DefaultConfig() Config {
	return Config{
		Level:            "info",
		Encoding:         "json",
		AddCaller:        false,
		CallerSkip:       1,
		Development:      false,
		StacktraceLevel:  "error",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		TimeFormat:       "iso8601",
		ColorOutput:      false,
	}
}

// ZapLogger zap 日志包装器
type ZapLogger struct {
	sugar *zap.SugaredLogger
}

// New 创建日志实例（简单版本，仅指定级别）
func New(level string) Logger {
	cfg := DefaultConfig()
	cfg.Level = level
	return NewWithConfig(cfg)
}

// NewWithConfig 使用配置创建日志实例
func NewWithConfig(cfg Config) Logger {
	zapCfg := buildZapConfig(cfg)

	var opts []zap.Option
	callerSkip := cfg.CallerSkip
	if callerSkip == 0 {
		callerSkip = 1
	}
	opts = append(opts, zap.AddCallerSkip(callerSkip))

	// 是否添加调用者信息
	if !cfg.AddCaller {
		zapCfg.DisableCaller = true
	}

	// 是否禁用堆栈跟踪
	if cfg.DisableStacktrace {
		zapCfg.DisableStacktrace = true
	}

	logger, err := zapCfg.Build(opts...)
	if err != nil {
		// fallback to nop logger
		return &ZapLogger{sugar: zap.NewNop().Sugar()}
	}

	return &ZapLogger{sugar: logger.Sugar()}
}

// buildZapConfig 构建 zap 配置
func buildZapConfig(cfg Config) zap.Config {
	var zapCfg zap.Config

	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	// 设置日志级别
	zapCfg.Level = zap.NewAtomicLevelAt(parseLevel(cfg.Level))

	// 设置输出格式
	encoding := strings.ToLower(cfg.Encoding)
	if encoding == "console" || encoding == "text" {
		zapCfg.Encoding = "console"
	} else {
		zapCfg.Encoding = "json"
	}

	// 设置时间格式
	zapCfg.EncoderConfig.TimeKey = "time"
	switch strings.ToLower(cfg.TimeFormat) {
	case "epoch":
		zapCfg.EncoderConfig.EncodeTime = zapcore.EpochTimeEncoder
	case "millis":
		zapCfg.EncoderConfig.EncodeTime = zapcore.EpochMillisTimeEncoder
	case "nanos":
		zapCfg.EncoderConfig.EncodeTime = zapcore.EpochNanosTimeEncoder
	case "rfc3339":
		zapCfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	case "rfc3339nano":
		zapCfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	default: // iso8601
		zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	// 设置堆栈跟踪级别
	zapCfg.DisableStacktrace = cfg.DisableStacktrace
	if !cfg.DisableStacktrace && cfg.StacktraceLevel != "" {
		zapCfg.EncoderConfig.StacktraceKey = "stacktrace"
	}

	// 设置彩色输出（仅 console 模式）
	if cfg.ColorOutput && zapCfg.Encoding == "console" {
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else if zapCfg.Encoding == "console" {
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	// 设置输出路径
	if len(cfg.OutputPaths) > 0 {
		zapCfg.OutputPaths = cfg.OutputPaths
	}
	if len(cfg.ErrorOutputPaths) > 0 {
		zapCfg.ErrorOutputPaths = cfg.ErrorOutputPaths
	}

	// 设置调用者格式（短路径）
	zapCfg.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	return zapCfg
}

// parseLevel 解析日志级别
func parseLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "dpanic":
		return zapcore.DPanicLevel
	case "panic":
		return zapcore.PanicLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

// NewFromEnv 从环境变量创建日志实例
// 支持的环境变量:
//   - LOG_LEVEL: 日志级别
//   - LOG_ENCODING: 输出格式 (json/console)
//   - LOG_ADD_CALLER: 是否添加调用者 (true/false)
//   - LOG_DEVELOPMENT: 开发模式 (true/false)
//   - LOG_COLOR: 彩色输出 (true/false)
func NewFromEnv() Logger {
	cfg := DefaultConfig()

	if level := os.Getenv("LOG_LEVEL"); level != "" {
		cfg.Level = level
	}
	if encoding := os.Getenv("LOG_ENCODING"); encoding != "" {
		cfg.Encoding = encoding
	}
	if addCaller := os.Getenv("LOG_ADD_CALLER"); addCaller == "true" || addCaller == "1" {
		cfg.AddCaller = true
	}
	if dev := os.Getenv("LOG_DEVELOPMENT"); dev == "true" || dev == "1" {
		cfg.Development = true
	}
	if color := os.Getenv("LOG_COLOR"); color == "true" || color == "1" {
		cfg.ColorOutput = true
	}

	return NewWithConfig(cfg)
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
func Debug(msg string, fields ...interface{})   { defaultLogger.Debug(msg, fields...) }
func Debugf(format string, args ...interface{}) { defaultLogger.Debugf(format, args...) }
func Info(msg string, fields ...interface{})    { defaultLogger.Info(msg, fields...) }
func Infof(format string, args ...interface{})  { defaultLogger.Infof(format, args...) }
func Warn(msg string, fields ...interface{})    { defaultLogger.Warn(msg, fields...) }
func Warnf(format string, args ...interface{})  { defaultLogger.Warnf(format, args...) }
func Error(msg string, fields ...interface{})   { defaultLogger.Error(msg, fields...) }
func Errorf(format string, args ...interface{}) { defaultLogger.Errorf(format, args...) }

// 兼容 fmt 包的便捷函数
func Printf(format string, args ...interface{}) {
	defaultLogger.Info(fmt.Sprintf(format, args...))
}

func Println(args ...interface{}) {
	defaultLogger.Info(fmt.Sprint(args...))
}

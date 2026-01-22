package zmsg

import "fmt"

// 错误定义
var (
	ErrNotFound       = &ZMsgError{Code: "NOT_FOUND", Message: "key not found"}
	ErrCacheFailed    = &ZMsgError{Code: "CACHE_FAILED", Message: "cache operation failed"}
	ErrDBFailed       = &ZMsgError{Code: "DB_FAILED", Message: "database operation failed"}
	ErrQueueFull      = &ZMsgError{Code: "QUEUE_FULL", Message: "queue is full"}
	ErrInvalidSQLTask = &ZMsgError{Code: "INVALID_SQL_TASK", Message: "invalid sql task"}
	ErrNotInitialized = &ZMsgError{Code: "NOT_INITIALIZED", Message: "zmsg not initialized"}
	ErrClosed         = &ZMsgError{Code: "CLOSED", Message: "zmsg is closed"}
	ErrTimeout        = &ZMsgError{Code: "TIMEOUT", Message: "operation timeout"}
)

// ZMsgError 自定义错误
type ZMsgError struct {
	Code    string
	Message string
	Err     error
}

func (e *ZMsgError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *ZMsgError) Unwrap() error {
	return e.Err
}

func ErrInvalidConfig(msg string) error {
	return &ZMsgError{
		Code:    "INVALID_CONFIG",
		Message: msg,
	}
}

func wrapError(code, msg string, err error) error {
	return &ZMsgError{
		Code:    code,
		Message: msg,
		Err:     err,
	}
}

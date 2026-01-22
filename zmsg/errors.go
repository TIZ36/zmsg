package zmsg

import "fmt"

// 错误定义
var (
	ErrNotFound = &ZMsgError{Code: "NOT_FOUND", Message: "key not found"}
	ErrClosed   = &ZMsgError{Code: "CLOSED", Message: "zmsg is closed"}
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

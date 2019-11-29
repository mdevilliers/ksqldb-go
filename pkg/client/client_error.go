package client

import "fmt"

type Error struct {
	ErrorCode  int      `json:"error_code"`
	Message    string   `json:"message"`
	StackTrace []string `json:"stackTrace"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d:%s", e.ErrorCode, e.Message)
}

func IsClientError(e error) bool {
	_, ok := e.(*Error)
	return ok
}

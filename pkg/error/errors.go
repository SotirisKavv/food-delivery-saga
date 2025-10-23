package error

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

type ServiceError int

const (
	ErrDatabaseError ServiceError = iota + 1
	ErrRepositoryError
	ErrPublishError
	ErrInternalError
	ErrBusinessError
)

func (e ServiceError) Error() string {
	switch e {
	case ErrDatabaseError:
		return "Database Error"
	case ErrRepositoryError:
		return "Repository Error"
	case ErrPublishError:
		return "Publish Error"
	case ErrInternalError:
		return "Internal Error"
	case ErrBusinessError:
		return "Error"
	default:
		return "Unknown Service Error"
	}
}

type ErrorDetails struct {
	Code      ServiceError
	Msg       string
	Op        string
	Cause     error
	OccuredAt time.Time
	Trace     []string
}

func (e *ErrorDetails) Error() string {
	base := e.Code.Error()
	if e.Msg != "" {
		base = base + ": " + e.Msg
	}
	if e.Op != "" {
		base = e.Op + ": " + base
	}
	return base
}

func (e *ErrorDetails) TraceString() string {
	if len(e.Trace) == 0 {
		if e.Op != "" {
			return e.Op
		}
		return ""
	}

	return strings.Join(e.Trace, ": ")
}

func (e *ErrorDetails) Is(target error) bool {
	if code, ok := target.(ServiceError); ok {
		return e.Code == code
	}

	return errors.Is(e.Cause, target)
}

func (e *ErrorDetails) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%s\n", e.Error())
			if ts := e.TraceString(); ts != "" {
				fmt.Fprintf(s, "trace: %s\n", ts)
			}
			if e.Cause != nil {
				fmt.Fprintf(s, "cause: %+v\n", e.Cause)
			}
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

func (e *ErrorDetails) Unwrap() error {
	return e.Cause
}

func New(code ServiceError, opts ...func(*ErrorDetails)) *ErrorDetails {
	err := &ErrorDetails{Code: code}
	for _, opt := range opts {
		opt(err)
	}
	if err.Op != "" {
		err.Trace = append(err.Trace, err.Op)
	}
	return err
}

func Newf(code ServiceError, format string, args ...any) *ErrorDetails {
	return New(code, WithMsg(fmt.Sprintf(format, args...)))
}

func WithMsg(msg string) func(*ErrorDetails) {
	return func(ed *ErrorDetails) { ed.Msg = msg }
}

func WithOp(op string) func(*ErrorDetails) {
	return func(ed *ErrorDetails) {
		ed.Op = op
		ed.Trace = append(ed.Trace, op)
	}
}

func WithCause(err error) func(*ErrorDetails) {
	return func(ed *ErrorDetails) { ed.Cause = err }
}

func WithTime(t time.Time) func(*ErrorDetails) {
	return func(ed *ErrorDetails) { ed.OccuredAt = t }
}

func AddOp(err error, op string) error {
	if err == nil {
		return nil
	}

	var ed *ErrorDetails
	if errors.As(err, &ed) {
		ed.Op = op
		ed.Trace = append(ed.Trace, op)
		return ed
	}

	return fmt.Errorf("%s: %w", op, err)
}

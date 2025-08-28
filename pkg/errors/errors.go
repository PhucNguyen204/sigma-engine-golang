package errors

import (
	"fmt"
)

type Result[T any] interface {
	IsOk() bool
	IsErr() bool
	Unwrap() T
	UnwrapErr() error
	UnwrapOr(defaultValue T) T
	Map(fn func(T) T) Result[T]
	MapErr(fn func(error) error) Result[T]
}

type okResult[T any] struct {
	value T
}

func (r okResult[T]) IsOk() bool                            { return true }
func (r okResult[T]) IsErr() bool                           { return false }
func (r okResult[T]) Unwrap() T                             { return r.value }
func (r okResult[T]) UnwrapErr() error                      { panic("called UnwrapErr on Ok result") }
func (r okResult[T]) UnwrapOr(defaultValue T) T             { return r.value }
func (r okResult[T]) Map(fn func(T) T) Result[T]            { return Ok(fn(r.value)) }
func (r okResult[T]) MapErr(fn func(error) error) Result[T] { return r }

type errResult[T any] struct {
	err error
}

func (r errResult[T]) IsOk() bool                            { return false }
func (r errResult[T]) IsErr() bool                           { return true }
func (r errResult[T]) Unwrap() T                             { panic("called Unwrap on Err result") }
func (r errResult[T]) UnwrapErr() error                      { return r.err }
func (r errResult[T]) UnwrapOr(defaultValue T) T             { return defaultValue }
func (r errResult[T]) Map(fn func(T) T) Result[T]            { return r }
func (r errResult[T]) MapErr(fn func(error) error) Result[T] { return Err[T](fn(r.err)) }

func Ok[T any](value T) Result[T] {
	return okResult[T]{value: value}
}

func Err[T any](err error) Result[T] {
	return errResult[T]{err: err}
}

func Try[T any](value T, err error) Result[T] {
	if err != nil {
		return Err[T](err)
	}
	return Ok(value)
}

func ToGoTuple[T any](result Result[T]) (T, error) {
	if result.IsOk() {
		return result.Unwrap(), nil
	} else {
		var zero T
		return zero, result.UnwrapErr()
	}
}

type ErrorType int

const (
	ErrorTypeCompilation        ErrorType = iota // SigmaError::CompilationError(String)
	ErrorTypeExecution                           // SigmaError::ExecutionError(String)
	ErrorTypeInvalidBytecode                     // SigmaError::InvalidBytecode(String)
	ErrorTypeInvalidPrimitiveID                  // SigmaError::InvalidPrimitiveId(u32)
	ErrorTypeStackUnderflow                      // SigmaError::StackUnderflow
	ErrorTypeStackOverflow                       // SigmaError::StackOverflow
	ErrorTypeIO                                  // SigmaError::IoError(String)
	ErrorTypeYAML                                // SigmaError::YamlError(String)

	// Matcher-related errors
	ErrorTypeUnsupportedMatchType
	ErrorTypeInvalidRegex
	ErrorTypeInvalidIPAddress
	ErrorTypeInvalidCIDR
	ErrorTypeInvalidNumber
	ErrorTypeInvalidRange
	ErrorTypeInvalidThreshold
	ErrorTypeModifier
	ErrorTypeFieldExtraction
	ErrorTypeExecutionTimeout
	ErrorTypeTooManyOperations
	ErrorTypeTooManyRegexOperations
	ErrorTypeBatchSizeMismatch
	ErrorTypeInvalidPrimitiveIndex
	ErrorTypeIncompatibleVersion

	// Advanced matcher errors
	ErrorTypeInvalidNumericValue
	ErrorTypeInvalidFieldPath
	ErrorTypeDangerousRegexPattern
)

func (et ErrorType) String() string {
	switch et {
	case ErrorTypeCompilation:
		return "COMPILATION"
	case ErrorTypeExecution:
		return "EXECUTION"
	case ErrorTypeInvalidBytecode:
		return "INVALID_BYTECODE"
	case ErrorTypeInvalidPrimitiveID:
		return "INVALID_PRIMITIVE_ID"
	case ErrorTypeStackUnderflow:
		return "STACK_UNDERFLOW"
	case ErrorTypeStackOverflow:
		return "STACK_OVERFLOW"
	case ErrorTypeIO:
		return "IO"
	case ErrorTypeYAML:
		return "YAML"
	case ErrorTypeUnsupportedMatchType:
		return "UNSUPPORTED_MATCH_TYPE"
	case ErrorTypeInvalidRegex:
		return "INVALID_REGEX"
	case ErrorTypeInvalidIPAddress:
		return "INVALID_IP_ADDRESS"
	case ErrorTypeInvalidCIDR:
		return "INVALID_CIDR"
	case ErrorTypeInvalidNumber:
		return "INVALID_NUMBER"
	case ErrorTypeInvalidRange:
		return "INVALID_RANGE"
	case ErrorTypeInvalidThreshold:
		return "INVALID_THRESHOLD"
	case ErrorTypeModifier:
		return "MODIFIER"
	case ErrorTypeFieldExtraction:
		return "FIELD_EXTRACTION"
	case ErrorTypeExecutionTimeout:
		return "EXECUTION_TIMEOUT"
	case ErrorTypeTooManyOperations:
		return "TOO_MANY_OPERATIONS"
	case ErrorTypeTooManyRegexOperations:
		return "TOO_MANY_REGEX_OPERATIONS"
	case ErrorTypeBatchSizeMismatch:
		return "BATCH_SIZE_MISMATCH"
	case ErrorTypeInvalidPrimitiveIndex:
		return "INVALID_PRIMITIVE_INDEX"
	case ErrorTypeIncompatibleVersion:
		return "INCOMPATIBLE_VERSION"
	case ErrorTypeInvalidNumericValue:
		return "INVALID_NUMERIC_VALUE"
	case ErrorTypeInvalidFieldPath:
		return "INVALID_FIELD_PATH"
	case ErrorTypeDangerousRegexPattern:
		return "DANGEROUS_REGEX_PATTERN"
	default:
		return "UNKNOWN"
	}
}

type SigmaError struct {
	Type         ErrorType `json:"type"`                    // Which error variant this represents
	Message      string    `json:"message"`                 // String data (for most variants)
	Details      string    `json:"details,omitempty"`       // Additional context
	NumericValue *uint64   `json:"numeric_value,omitempty"` // For u32/u64 data in Rust variants
	Cause        error     `json:"-"`                       // Wrapped error (equivalent to error chaining)
}

func (e *SigmaError) Error() string {
	switch e.Type {
	case ErrorTypeCompilation:
		return fmt.Sprintf("Compilation error: %s", e.Message)
	case ErrorTypeExecution:
		return fmt.Sprintf("Execution error: %s", e.Message)
	case ErrorTypeInvalidBytecode:
		return fmt.Sprintf("Invalid bytecode: %s", e.Message)
	case ErrorTypeInvalidPrimitiveID:
		if e.NumericValue != nil {
			return fmt.Sprintf("Invalid primitive ID: %d", *e.NumericValue)
		}
		return fmt.Sprintf("Invalid primitive ID: %s", e.Message)
	case ErrorTypeStackUnderflow:
		return "Stack underflow during execution"
	case ErrorTypeStackOverflow:
		return "Stack overflow during execution"
	case ErrorTypeIO:
		return fmt.Sprintf("IO error: %s", e.Message)
	case ErrorTypeYAML:
		return fmt.Sprintf("YAML parsing error: %s", e.Message)
	case ErrorTypeUnsupportedMatchType:
		return fmt.Sprintf("Unsupported match type: %s", e.Message)
	case ErrorTypeInvalidRegex:
		return fmt.Sprintf("Invalid regex pattern: %s", e.Message)
	case ErrorTypeInvalidIPAddress:
		return fmt.Sprintf("Invalid IP address: %s", e.Message)
	case ErrorTypeInvalidCIDR:
		return fmt.Sprintf("Invalid CIDR notation: %s", e.Message)
	case ErrorTypeInvalidNumber:
		return fmt.Sprintf("Invalid number: %s", e.Message)
	case ErrorTypeInvalidRange:
		return fmt.Sprintf("Invalid range: %s", e.Message)
	case ErrorTypeInvalidThreshold:
		return fmt.Sprintf("Invalid threshold: %s", e.Message)
	case ErrorTypeModifier:
		return fmt.Sprintf("Modifier error: %s", e.Message)
	case ErrorTypeFieldExtraction:
		return fmt.Sprintf("Field extraction error: %s", e.Message)
	case ErrorTypeExecutionTimeout:
		return "Execution timeout exceeded"
	case ErrorTypeTooManyOperations:
		if e.NumericValue != nil {
			return fmt.Sprintf("Too many operations: %d", *e.NumericValue)
		}
		return fmt.Sprintf("Too many operations: %s", e.Message)
	case ErrorTypeTooManyRegexOperations:
		if e.NumericValue != nil {
			return fmt.Sprintf("Too many regex operations: %d", *e.NumericValue)
		}
		return fmt.Sprintf("Too many regex operations: %s", e.Message)
	case ErrorTypeBatchSizeMismatch:
		return "Batch size mismatch"
	case ErrorTypeInvalidPrimitiveIndex:
		if e.NumericValue != nil {
			return fmt.Sprintf("Invalid primitive index: %d", *e.NumericValue)
		}
		return fmt.Sprintf("Invalid primitive index: %s", e.Message)
	case ErrorTypeIncompatibleVersion:
		if e.NumericValue != nil {
			return fmt.Sprintf("Incompatible version: %d", *e.NumericValue)
		}
		return fmt.Sprintf("Incompatible version: %s", e.Message)
	case ErrorTypeInvalidNumericValue:
		return fmt.Sprintf("Invalid numeric value: %s", e.Message)
	case ErrorTypeInvalidFieldPath:
		return fmt.Sprintf("Invalid field path: %s", e.Message)
	case ErrorTypeDangerousRegexPattern:
		return fmt.Sprintf("Dangerous regex pattern detected: %s", e.Message)
	default:
		return fmt.Sprintf("Unknown error: %s", e.Message)
	}
}

func (e *SigmaError) Unwrap() error {
	return e.Cause
}

func (e *SigmaError) Is(target error) bool {
	if target == nil {
		return false
	}

	if other, ok := target.(*SigmaError); ok {
		return e.Type == other.Type && e.Message == other.Message
	}

	return false
}

func New(errType ErrorType, message string) *SigmaError {
	return &SigmaError{
		Type:    errType,
		Message: message,
	}
}

func NewWithNumeric(errType ErrorType, message string, value uint64) *SigmaError {
	return &SigmaError{
		Type:         errType,
		Message:      message,
		NumericValue: &value,
	}
}

func Wrap(errType ErrorType, message string, cause error) *SigmaError {
	return &SigmaError{
		Type:    errType,
		Message: message,
		Cause:   cause,
	}
}

func NewCompilationError(message string) *SigmaError {
	return New(ErrorTypeCompilation, message)
}

func NewExecutionError(message string) *SigmaError {
	return New(ErrorTypeExecution, message)
}

func NewInvalidBytecode(message string) *SigmaError {
	return New(ErrorTypeInvalidBytecode, message)
}

func NewInvalidPrimitiveID(id uint32) *SigmaError {
	return NewWithNumeric(ErrorTypeInvalidPrimitiveID, "", uint64(id))
}

func NewStackUnderflow() *SigmaError {
	return New(ErrorTypeStackUnderflow, "")
}

func NewStackOverflow() *SigmaError {
	return New(ErrorTypeStackOverflow, "")
}

func NewIOError(message string) *SigmaError {
	return New(ErrorTypeIO, message)
}

func NewYAMLError(message string) *SigmaError {
	return New(ErrorTypeYAML, message)
}

func NewUnsupportedMatchType(matchType string) *SigmaError {
	return New(ErrorTypeUnsupportedMatchType, matchType)
}

func NewInvalidRegex(pattern string) *SigmaError {
	return New(ErrorTypeInvalidRegex, pattern)
}

func NewInvalidIPAddress(ip string) *SigmaError {
	return New(ErrorTypeInvalidIPAddress, ip)
}

func NewInvalidCIDR(cidr string) *SigmaError {
	return New(ErrorTypeInvalidCIDR, cidr)
}

func NewInvalidNumber(number string) *SigmaError {
	return New(ErrorTypeInvalidNumber, number)
}

func NewInvalidRange(rangeStr string) *SigmaError {
	return New(ErrorTypeInvalidRange, rangeStr)
}

func NewInvalidThreshold(threshold string) *SigmaError {
	return New(ErrorTypeInvalidThreshold, threshold)
}

func NewModifierError(message string) *SigmaError {
	return New(ErrorTypeModifier, message)
}

func NewFieldExtractionError(message string) *SigmaError {
	return New(ErrorTypeFieldExtraction, message)
}

func NewExecutionTimeout() *SigmaError {
	return New(ErrorTypeExecutionTimeout, "")
}

func NewTooManyOperations(count uint64) *SigmaError {
	return NewWithNumeric(ErrorTypeTooManyOperations, "", count)
}

func NewTooManyRegexOperations(count uint64) *SigmaError {
	return NewWithNumeric(ErrorTypeTooManyRegexOperations, "", count)
}

func NewBatchSizeMismatch() *SigmaError {
	return New(ErrorTypeBatchSizeMismatch, "")
}

func NewInvalidPrimitiveIndex(index uint64) *SigmaError {
	return NewWithNumeric(ErrorTypeInvalidPrimitiveIndex, "", index)
}

func NewIncompatibleVersion(version uint32) *SigmaError {
	return NewWithNumeric(ErrorTypeIncompatibleVersion, "", uint64(version))
}

func NewInvalidNumericValue(value string) *SigmaError {
	return New(ErrorTypeInvalidNumericValue, value)
}

func NewInvalidFieldPath(path string) *SigmaError {
	return New(ErrorTypeInvalidFieldPath, path)
}

func NewDangerousRegexPattern(pattern string) *SigmaError {
	return New(ErrorTypeDangerousRegexPattern, pattern)
}

func WrapIOError(err error) *SigmaError {
	if err == nil {
		return nil
	}
	return Wrap(ErrorTypeIO, err.Error(), err)
}

func WrapYAMLError(err error) *SigmaError {
	if err == nil {
		return nil
	}
	return Wrap(ErrorTypeYAML, err.Error(), err)
}

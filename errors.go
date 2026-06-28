package mlflow

import (
	"errors"
	"fmt"
)

// APIError is a non-2xx response from MLflow, mapped from its
// {"error_code","message"} envelope.
type APIError struct {
	Code       string // MLflow error_code, e.g. "RESOURCE_DOES_NOT_EXIST"
	Message    string
	HTTPStatus int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("mlflow: %s (%d): %s", e.Code, e.HTTPStatus, e.Message)
}

// IsNotFound reports whether err is an APIError with RESOURCE_DOES_NOT_EXIST.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == "RESOURCE_DOES_NOT_EXIST"
}

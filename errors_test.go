package mlflow

import (
	"errors"
	"testing"
)

func TestAPIErrorAndIsNotFound(t *testing.T) {
	e := &APIError{Code: "RESOURCE_DOES_NOT_EXIST", Message: "no such run", HTTPStatus: 404}
	if got := e.Error(); got != "mlflow: RESOURCE_DOES_NOT_EXIST (404): no such run" {
		t.Fatalf("Error() = %q", got)
	}
	if !IsNotFound(e) {
		t.Fatal("IsNotFound should be true for RESOURCE_DOES_NOT_EXIST")
	}
	if IsNotFound(&APIError{Code: "INVALID_PARAMETER_VALUE"}) {
		t.Fatal("IsNotFound should be false for other codes")
	}
	if IsNotFound(errors.New("plain")) {
		t.Fatal("IsNotFound should be false for non-APIError")
	}
}

package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/cofy-x/kova/internal/logging"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	"github.com/labstack/echo/v4"
)

func writeAPIError(c echo.Context, status int, code apiv1.ErrorCode, message string, retryable bool, retryAfter time.Duration) error {
	if retryAfter > 0 {
		seconds := int64(retryAfter / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		c.Response().Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	}
	return c.JSON(status, apiv1.ErrorResponse{Code: code, Message: message, Retryable: retryable})
}

func invalidRequest(c echo.Context, err error) error {
	return writeAPIError(c, http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, err.Error(), false, 0)
}

func notFound(c echo.Context) error {
	return writeAPIError(c, http.StatusNotFound, apiv1.ErrorCodeNotFound, "build not found", false, 0)
}

func internalError(c echo.Context, err error) error {
	return writeInternalError(c, err, true)
}

func internalContractError(c echo.Context, err error) error {
	return writeInternalError(c, err, false)
}

func writeInternalError(c echo.Context, err error, retryable bool) error {
	logging.Errorf("Service request %s %s failed: %v", c.Request().Method, c.Request().URL.Path, err)
	return writeAPIError(c, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "internal service error", retryable, 0)
}

func serviceUnavailable(c echo.Context, err error) error {
	logging.Errorf("Service readiness check failed: %v", err)
	return writeAPIError(c, http.StatusServiceUnavailable, apiv1.ErrorCodeInternal, "service is not ready", true, 5*time.Second)
}

func conflict(c echo.Context, message string) error {
	return writeAPIError(c, http.StatusConflict, apiv1.ErrorCodeConflict, message, false, 0)
}

func queueCapacityExceeded(c echo.Context) error {
	return writeAPIError(c, http.StatusTooManyRequests, apiv1.ErrorCodeQueueCapacityExceeded, "requester queue limit is reached", true, 5*time.Second)
}

func logsUnavailable(c echo.Context, status int, message string, retryable bool) error {
	return writeAPIError(c, status, apiv1.ErrorCodeLogsUnavailable, message, retryable, 0)
}

func (s *Server) httpErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}
	var httpErr *echo.HTTPError
	if !errors.As(err, &httpErr) {
		_ = internalError(c, err)
		return
	}
	switch httpErr.Code {
	case http.StatusNotFound:
		_ = writeAPIError(c, http.StatusNotFound, apiv1.ErrorCodeNotFound, "endpoint not found", false, 0)
	case http.StatusMethodNotAllowed:
		_ = writeAPIError(c, http.StatusMethodNotAllowed, apiv1.ErrorCodeInvalidRequest, "method not allowed", false, 0)
	default:
		_ = internalError(c, err)
	}
}

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
)

type APIError struct {
	StatusCode int
	Code       apiv1.ErrorCode
	Message    string
	Retryable  bool
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Kova service API %s (HTTP %d): %s", e.Code, e.StatusCode, e.Message)
}

func (c *Client) getJSON(ctx context.Context, endpoint string, out any) error {
	req, err := c.request(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) getBytes(ctx context.Context, endpoint string) ([]byte, error) {
	req, err := c.request(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, responseError(resp)
	}
	return readBounded(resp.Body, c.maxResponseBytes)
}

func (c *Client) request(ctx context.Context, method, endpoint string, body io.Reader) (*http.Request, error) {
	relative, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + relative.Path
	u.RawQuery = relative.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

func (c *Client) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError(resp)
	}
	if out == nil {
		return nil
	}
	raw, err := readBounded(resp.Body, c.maxResponseBytes)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode service response: %w", err)
	}
	return nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("service response exceeds %d bytes", limit)
	}
	return raw, nil
}

func responseError(resp *http.Response) error {
	payload := apiv1.ErrorResponse{}
	structured := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload) == nil && payload.Code != ""
	if payload.Code == "" {
		payload.Code = defaultErrorCode(resp.StatusCode)
		if !structured && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError) {
			payload.Retryable = true
		}
	}
	if strings.TrimSpace(payload.Message) == "" {
		payload.Message = http.StatusText(resp.StatusCode)
	}
	return &APIError{
		StatusCode: resp.StatusCode,
		Code:       payload.Code,
		Message:    strings.TrimSpace(payload.Message),
		Retryable:  payload.Retryable,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
}

func defaultErrorCode(status int) apiv1.ErrorCode {
	switch status {
	case http.StatusBadRequest, http.StatusMethodNotAllowed:
		return apiv1.ErrorCodeInvalidRequest
	case http.StatusUnauthorized:
		return apiv1.ErrorCodeUnauthenticated
	case http.StatusForbidden:
		return apiv1.ErrorCodeForbidden
	case http.StatusNotFound:
		return apiv1.ErrorCodeNotFound
	case http.StatusConflict:
		return apiv1.ErrorCodeConflict
	case http.StatusTooManyRequests:
		return apiv1.ErrorCodeQueueCapacityExceeded
	case http.StatusGone:
		return apiv1.ErrorCodeLogsUnavailable
	default:
		return apiv1.ErrorCodeInternal
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

package serviceclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

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
	return io.ReadAll(resp.Body)
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
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode service response: %w", err)
	}
	return nil
}

func responseError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &payload)
	message := strings.TrimSpace(payload.Error)
	if message == "" {
		message = strings.TrimSpace(string(raw))
	}
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return fmt.Errorf("Kova service returned %s: %s", resp.Status, message)
}

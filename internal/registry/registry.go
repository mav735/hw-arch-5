package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	URL  string
	HTTP *http.Client
}

type Subject struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

func New(url string) *Client {
	return &Client{URL: url, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	return c.call(ctx, http.MethodPut, "/config/"+subject, map[string]string{"compatibility": level}, nil)
}

func (c *Client) Register(ctx context.Context, subject, schema string) (int, error) {
	var out struct {
		ID int `json:"id"`
	}
	err := c.call(ctx, http.MethodPost, "/subjects/"+subject+"/versions",
		map[string]string{"schema": schema, "schemaType": "AVRO"}, &out)
	return out.ID, err
}

func (c *Client) Latest(ctx context.Context, subject string) (Subject, error) {
	var s Subject
	err := c.call(ctx, http.MethodGet, "/subjects/"+subject+"/versions/latest", nil, &s)
	return s, err
}

func (c *Client) call(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.URL+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("schema registry %s %s: %d %s", method, path, resp.StatusCode, msg)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

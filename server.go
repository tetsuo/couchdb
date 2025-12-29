package couchdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// ServerService provides methods for server-level operations.
type ServerService struct {
	client *Client
}

// NewServerService creates a new ServerService.
func NewServerService(client *Client) *ServerService {
	return &ServerService{client: client}
}

// UUIDsResponse represents the response from the _uuids endpoint.
type UUIDsResponse struct {
	UUIDs []string `json:"uuids"`
}

// GetUUIDs requests one or more UUIDs from the server.
func (s *ServerService) GetUUIDs(ctx context.Context, count int, opts ...RequestOption) (*UUIDsResponse, error) {
	path := "/_uuids"

	if count > 0 {
		query := url.Values{}
		query.Set("count", fmt.Sprintf("%d", count))
		path = fmt.Sprintf("%s?%s", path, query.Encode())
	}

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get UUIDs: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to get UUIDs: %s - %s", errResp.Error, errResp.Reason)
	}

	var uuidsResp UUIDsResponse
	if err := json.Unmarshal(body, &uuidsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &uuidsResp, nil
}

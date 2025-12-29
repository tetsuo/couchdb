package couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// SecurityService provides methods for managing database security.
type SecurityService struct {
	client *Client
}

// NewSecurityService creates a new SecurityService.
func NewSecurityService(client *Client) *SecurityService {
	return &SecurityService{client: client}
}

// SecurityObject represents the security object for a database.
type SecurityObject struct {
	Admins  Members `json:"admins"`
	Members Members `json:"members"`
}

// Members represents the members/admins section of the security object.
type Members struct {
	Names []string `json:"names"`
	Roles []string `json:"roles"`
}

// GetSecurity retrieves the security object for a database.
func (s *SecurityService) GetSecurity(ctx context.Context, dbName string, opts ...RequestOption) (*SecurityObject, error) {
	path := fmt.Sprintf("/%s/_security", url.PathEscape(dbName))

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
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
		return nil, fmt.Errorf("failed to get security: %s - %s", errResp.Error, errResp.Reason)
	}

	var security SecurityObject
	if err := json.Unmarshal(body, &security); err != nil {
		return nil, fmt.Errorf("failed to unmarshal security: %w", err)
	}

	return &security, nil
}

// SetSecurity sets the security object for a database.
func (s *SecurityService) SetSecurity(ctx context.Context, dbName string, security *SecurityObject, opts ...RequestOption) error {
	path := fmt.Sprintf("/%s/_security", url.PathEscape(dbName))

	data, err := json.Marshal(security)
	if err != nil {
		return fmt.Errorf("failed to marshal security: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data), opts...)
	if err != nil {
		return fmt.Errorf("failed to set security: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return fmt.Errorf("failed to set security: %s - %s", errResp.Error, errResp.Reason)
	}

	return nil
}

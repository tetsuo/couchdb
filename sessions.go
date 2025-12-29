package couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// SessionService provides methods for session-based authentication.
type SessionService struct {
	client *Client
}

// NewSessionService creates a new SessionService.
func NewSessionService(client *Client) *SessionService {
	return &SessionService{client: client}
}

// AuthInfo represents authentication information in a session.
type AuthInfo struct {
	Authenticated          string   `json:"authenticated"`
	AuthenticationDB       string   `json:"authentication_db"`
	AuthenticationHandlers []string `json:"authentication_handlers"`
}

// SessionInfo represents information about the current session.
type SessionInfo struct {
	OK      bool        `json:"ok"`
	Info    AuthInfo    `json:"info"`
	UserCtx UserContext `json:"userCtx"`
}

// UserContext represents the user context in a session.
type UserContext struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

// LoginResponse represents the response from a login request.
type LoginResponse struct {
	OK    bool     `json:"ok"`
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

// Login authenticates a user and creates a session.
func (s *SessionService) Login(ctx context.Context, username, password string, opts ...RequestOption) (*LoginResponse, *http.Cookie, error) {
	credentials := map[string]string{
		"name":     username,
		"password": password,
	}

	data, err := json.Marshal(credentials)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal credentials: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPost, "/_session", bytes.NewReader(data), opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to login: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, nil, fmt.Errorf("failed to login: %s - %s", errResp.Error, errResp.Reason)
	}

	var loginResp LoginResponse
	if err := json.Unmarshal(body, &loginResp); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Extract the session cookie.
	var sessionCookie *http.Cookie
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "AuthSession" {
			sessionCookie = cookie
			break
		}
	}

	return &loginResp, sessionCookie, nil
}

// Logout ends the current session.
func (s *SessionService) Logout(ctx context.Context, opts ...RequestOption) error {
	resp, err := s.client.doRequest(ctx, http.MethodDelete, "/_session", nil, opts...)
	if err != nil {
		return fmt.Errorf("failed to logout: %w", err)
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
		return fmt.Errorf("failed to logout: %s - %s", errResp.Error, errResp.Reason)
	}

	return nil
}

// GetSession retrieves information about the current session.
func (s *SessionService) GetSession(ctx context.Context, opts ...RequestOption) (*SessionInfo, error) {
	resp, err := s.client.doRequest(ctx, http.MethodGet, "/_session", nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
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
		return nil, fmt.Errorf("failed to get session: %s - %s", errResp.Error, errResp.Reason)
	}

	var sessionInfo SessionInfo
	if err := json.Unmarshal(body, &sessionInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session info: %w", err)
	}

	return &sessionInfo, nil
}

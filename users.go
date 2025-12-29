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

// UserService provides methods for managing CouchDB users.
type UserService struct {
	client *Client
}

// NewUserService creates a new UserService.
func NewUserService(client *Client) *UserService {
	return &UserService{client: client}
}

// User represents a CouchDB user document.
type User struct {
	ID             string   `json:"_id,omitempty"`
	Rev            string   `json:"_rev,omitempty"`
	Name           string   `json:"name"`
	Type           string   `json:"type"`
	Roles          []string `json:"roles"`
	Password       string   `json:"password,omitempty"`
	Salt           string   `json:"salt,omitempty"`
	DerivedKey     string   `json:"derived_key,omitempty"`
	Iterations     int      `json:"iterations,omitempty"`
	PasswordScheme string   `json:"password_scheme,omitempty"`
}

// UserResponse represents the response from CouchDB for user operations.
type UserResponse struct {
	OK  bool   `json:"ok"`
	ID  string `json:"id"`
	Rev string `json:"rev"`
}

// CreateUser creates a new user in the _users database.
func (s *UserService) CreateUser(ctx context.Context, name, password string, roles []string, opts ...RequestOption) (*UserResponse, error) {
	user := User{
		ID:       fmt.Sprintf("org.couchdb.user:%s", name),
		Name:     name,
		Type:     "user",
		Roles:    roles,
		Password: password,
	}

	if user.Roles == nil {
		user.Roles = []string{}
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal user: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPost, "/_users", bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to create user: %s - %s", errResp.Error, errResp.Reason)
	}

	var userResp UserResponse
	if err := json.Unmarshal(body, &userResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &userResp, nil
}

// GetUser retrieves a user from the _users database.
func (s *UserService) GetUser(ctx context.Context, name string, opts ...RequestOption) (*User, error) {
	docID := fmt.Sprintf("org.couchdb.user:%s", name)
	path := fmt.Sprintf("/_users/%s", url.PathEscape(docID))

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("user not found: %s", name)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to get user: %s - %s", errResp.Error, errResp.Reason)
	}

	var user User
	if err := json.Unmarshal(body, &user); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user: %w", err)
	}

	return &user, nil
}

// UpdateUser updates an existing user in the _users database.
func (s *UserService) UpdateUser(ctx context.Context, name, rev string, password *string, roles []string, opts ...RequestOption) (*UserResponse, error) {
	docID := fmt.Sprintf("org.couchdb.user:%s", name)

	user := User{
		ID:    docID,
		Rev:   rev,
		Name:  name,
		Type:  "user",
		Roles: roles,
	}

	if password != nil {
		user.Password = *password
	}

	if user.Roles == nil {
		user.Roles = []string{}
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal user: %w", err)
	}

	path := fmt.Sprintf("/_users/%s", url.PathEscape(docID))
	resp, err := s.client.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to update user: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to update user: %s - %s", errResp.Error, errResp.Reason)
	}

	var userResp UserResponse
	if err := json.Unmarshal(body, &userResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &userResp, nil
}

// DeleteUser deletes a user from the _users database.
func (s *UserService) DeleteUser(ctx context.Context, name, rev string, opts ...RequestOption) (*UserResponse, error) {
	docID := fmt.Sprintf("org.couchdb.user:%s", name)
	path := fmt.Sprintf("/_users/%s?rev=%s", url.PathEscape(docID), url.QueryEscape(rev))

	resp, err := s.client.doRequest(ctx, http.MethodDelete, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to delete user: %w", err)
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
		return nil, fmt.Errorf("failed to delete user: %s - %s", errResp.Error, errResp.Reason)
	}

	var userResp UserResponse
	if err := json.Unmarshal(body, &userResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &userResp, nil
}

// ListUsers retrieves all users from the _users database.
func (s *UserService) ListUsers(ctx context.Context, opts ...RequestOption) ([]User, error) {
	path := "/_users/_all_docs?include_docs=true"

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
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
		return nil, fmt.Errorf("failed to list users: %s - %s", errResp.Error, errResp.Reason)
	}

	var result struct {
		Rows []struct {
			Doc User `json:"doc"`
		} `json:"rows"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	users := make([]User, 0, len(result.Rows))
	for _, row := range result.Rows {
		// Filter out design documents and only include user documents.
		if row.Doc.Type == "user" {
			users = append(users, row.Doc)
		}
	}

	return users, nil
}

// UpdatePassword updates only the password for an existing user.
func (s *UserService) UpdatePassword(ctx context.Context, name, rev, newPassword string, opts ...RequestOption) (*UserResponse, error) {
	// Get current user to preserve roles.
	user, err := s.GetUser(ctx, name, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return s.UpdateUser(ctx, name, rev, &newPassword, user.Roles, opts...)
}

// UpdateRoles updates only the roles for an existing user.
func (s *UserService) UpdateRoles(ctx context.Context, name, rev string, roles []string, opts ...RequestOption) (*UserResponse, error) {
	// Get current user to preserve password hash.
	currentUser, err := s.GetUser(ctx, name, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	docID := fmt.Sprintf("org.couchdb.user:%s", name)

	// Preserve all password-related fields from the current user.
	user := User{
		ID:             docID,
		Rev:            rev,
		Name:           name,
		Type:           "user",
		Roles:          roles,
		Salt:           currentUser.Salt,
		DerivedKey:     currentUser.DerivedKey,
		Iterations:     currentUser.Iterations,
		PasswordScheme: currentUser.PasswordScheme,
	}

	if user.Roles == nil {
		user.Roles = []string{}
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal user: %w", err)
	}

	path := fmt.Sprintf("/_users/%s", url.PathEscape(docID))
	resp, err := s.client.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to update roles: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to update roles: %s - %s", errResp.Error, errResp.Reason)
	}

	var userResp UserResponse
	if err := json.Unmarshal(body, &userResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &userResp, nil
}

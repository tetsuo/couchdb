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

// ConfigurationService provides methods for managing CouchDB configuration.
// See: https://docs.couchdb.org/en/stable/api/server/configuration.html
//
// Note: CouchDB configuration API returns and accepts JSON-encoded string values.
// For example, setting a value to "5" requires sending the JSON string "\"5\"".
//
// Node Management:
// All methods accept a nodeName parameter to configure specific nodes in a cluster.
// Use "_local" to configure the local node (most common case).
type ConfigurationService struct {
	client *Client
}

// NewConfigurationService creates a new ConfigurationService.
func NewConfigurationService(client *Client) *ConfigurationService {
	return &ConfigurationService{client: client}
}

// GetConfiguration returns the entire CouchDB configuration as a nested map.
// GET /_node/{node-name}/_config
func (s *ConfigurationService) GetConfiguration(ctx context.Context, nodeName string, opts ...RequestOption) (map[string]map[string]string, error) {
	path := fmt.Sprintf("/_node/%s/_config", url.PathEscape(nodeName))

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get configuration: %w", err)
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
		return nil, fmt.Errorf("failed to get configuration: %s - %s", errResp.Error, errResp.Reason)
	}

	var config map[string]map[string]string
	if err := json.Unmarshal(body, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration: %w", err)
	}

	return config, nil
}

// GetConfigurationSection gets the configuration structure for a single section.
// GET /_node/{node-name}/_config/{section}
func (s *ConfigurationService) GetConfigurationSection(ctx context.Context, nodeName, section string, opts ...RequestOption) (map[string]string, error) {
	path := fmt.Sprintf("/_node/%s/_config/%s", url.PathEscape(nodeName), url.PathEscape(section))

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get configuration section: %w", err)
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
		return nil, fmt.Errorf("failed to get configuration section: %s - %s", errResp.Error, errResp.Reason)
	}

	var sectionConfig map[string]string
	if err := json.Unmarshal(body, &sectionConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration section: %w", err)
	}

	return sectionConfig, nil
}

// GetConfigurationValue gets a single configuration value from within a specific section.
// GET /_node/{node-name}/_config/{section}/{key}
func (s *ConfigurationService) GetConfigurationValue(ctx context.Context, nodeName, section, key string, opts ...RequestOption) (string, error) {
	path := fmt.Sprintf("/_node/%s/_config/%s/%s", url.PathEscape(nodeName), url.PathEscape(section), url.PathEscape(key))

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return "", fmt.Errorf("failed to get configuration value: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return "", fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return "", fmt.Errorf("failed to get configuration value: %s - %s", errResp.Error, errResp.Reason)
	}

	var value string
	if err := json.Unmarshal(body, &value); err != nil {
		return "", fmt.Errorf("failed to unmarshal configuration value: %w", err)
	}

	return value, nil
}

// SetConfigurationValue sets a single configuration value.
// PUT /_node/{node-name}/_config/{section}/{key}
// Returns the old value if it existed.
func (s *ConfigurationService) SetConfigurationValue(ctx context.Context, nodeName, section, key, value string, opts ...RequestOption) (string, error) {
	path := fmt.Sprintf("/_node/%s/_config/%s/%s", url.PathEscape(nodeName), url.PathEscape(section), url.PathEscape(key))

	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal value: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data), opts...)
	if err != nil {
		return "", fmt.Errorf("failed to set configuration value: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return "", fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return "", fmt.Errorf("failed to set configuration value: %s - %s", errResp.Error, errResp.Reason)
	}

	var oldValue string
	if err := json.Unmarshal(body, &oldValue); err != nil {
		return "", fmt.Errorf("failed to unmarshal old value: %w", err)
	}

	return oldValue, nil
}

// DeleteConfigurationValue deletes a configuration value.
// DELETE /_node/{node-name}/_config/{section}/{key}
// Returns the deleted value.
func (s *ConfigurationService) DeleteConfigurationValue(ctx context.Context, nodeName, section, key string, opts ...RequestOption) (string, error) {
	path := fmt.Sprintf("/_node/%s/_config/%s/%s", url.PathEscape(nodeName), url.PathEscape(section), url.PathEscape(key))

	resp, err := s.client.doRequest(ctx, http.MethodDelete, path, nil, opts...)
	if err != nil {
		return "", fmt.Errorf("failed to delete configuration value: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return "", fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return "", fmt.Errorf("failed to delete configuration value: %s - %s", errResp.Error, errResp.Reason)
	}

	var deletedValue string
	if err := json.Unmarshal(body, &deletedValue); err != nil {
		return "", fmt.Errorf("failed to unmarshal deleted value: %w", err)
	}

	return deletedValue, nil
}

// ReloadConfiguration reloads the configuration from disk.
// POST /_node/{node-name}/_config/_reload
func (s *ConfigurationService) ReloadConfiguration(ctx context.Context, nodeName string, opts ...RequestOption) error {
	path := fmt.Sprintf("/_node/%s/_config/_reload", url.PathEscape(nodeName))

	resp, err := s.client.doRequest(ctx, http.MethodPost, path, nil, opts...)
	if err != nil {
		return fmt.Errorf("failed to reload configuration: %w", err)
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
		return fmt.Errorf("failed to reload configuration: %s - %s", errResp.Error, errResp.Reason)
	}

	return nil
}

// Admin management convenience methods.
// These operate on the "admins" section of the configuration.

// CreateAdmin creates a new admin user by setting their password in the admins section.
// This is a convenience wrapper around SetConfigurationValue.
func (s *ConfigurationService) CreateAdmin(ctx context.Context, nodeName, username, password string, opts ...RequestOption) error {
	_, err := s.SetConfigurationValue(ctx, nodeName, "admins", username, password, opts...)
	return err
}

// DeleteAdmin removes an admin user.
// This is a convenience wrapper around DeleteConfigurationValue.
func (s *ConfigurationService) DeleteAdmin(ctx context.Context, nodeName, username string, opts ...RequestOption) error {
	_, err := s.DeleteConfigurationValue(ctx, nodeName, "admins", username, opts...)
	return err
}

// UpdateAdminPassword updates an admin's password.
// This is a convenience wrapper around SetConfigurationValue.
func (s *ConfigurationService) UpdateAdminPassword(ctx context.Context, nodeName, username, newPassword string, opts ...RequestOption) error {
	_, err := s.SetConfigurationValue(ctx, nodeName, "admins", username, newPassword, opts...)
	return err
}

// GetAdmins returns all admin usernames and their password hashes.
// This is a convenience wrapper around GetConfigurationSection.
func (s *ConfigurationService) GetAdmins(ctx context.Context, nodeName string, opts ...RequestOption) (map[string]string, error) {
	return s.GetConfigurationSection(ctx, nodeName, "admins", opts...)
}

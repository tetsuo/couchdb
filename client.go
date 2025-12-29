// Package couchdb provides a client library for interacting with the CouchDB API (v3.5.x).
package couchdb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Authenticator defines the interface for different authentication methods.
type Authenticator interface {
	// Authenticate adds authentication credentials to the request.
	Authenticate(req *http.Request) error
}

// BasicAuthenticator implements HTTP Basic Authentication.
type BasicAuthenticator struct {
	Username string
	Password string
}

func (a *BasicAuthenticator) Authenticate(req *http.Request) error {
	req.SetBasicAuth(a.Username, a.Password)
	return nil
}

// CookieAuthenticator implements Cookie-based Authentication.
type CookieAuthenticator struct {
	Cookie *http.Cookie
}

func (a *CookieAuthenticator) Authenticate(req *http.Request) error {
	if a.Cookie != nil {
		req.AddCookie(a.Cookie)
	}
	return nil
}

// ProxyAuthenticator implements Proxy Authentication.
// Sets X-Auth-CouchDB-UserName, X-Auth-CouchDB-Roles, and X-Auth-CouchDB-Token headers.
type ProxyAuthenticator struct {
	Roles    []string
	Username string
	Token    string // Secret token shared between proxy and CouchDB
}

func (a *ProxyAuthenticator) Authenticate(req *http.Request) error {
	req.Header.Set("X-Auth-CouchDB-UserName", a.Username)
	if len(a.Roles) > 0 {
		req.Header.Set("X-Auth-CouchDB-Roles", strings.Join(a.Roles, ","))
	}
	if a.Token != "" {
		req.Header.Set("X-Auth-CouchDB-Token", a.Token)
	}
	return nil
}

// JWTAuthenticator implements JWT Bearer Token Authentication.
type JWTAuthenticator struct {
	Token string
}

func (a *JWTAuthenticator) Authenticate(req *http.Request) error {
	if a.Token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.Token))
	}
	return nil
}

// Client is an HTTP client for interacting with a CouchDB server.
type Client struct {
	baseURL string
	client  *http.Client
}

// ClientOption is a functional option for configuring CouchDBClient.
type ClientOption func(*Client)

// RequestOption is a functional option for configuring individual requests.
type RequestOption func() Authenticator

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *Client) {
		c.client = client
	}
}

// WithBasicAuth configures the request to use HTTP Basic Authentication.
func WithBasicAuth(username, password string) RequestOption {
	return func() Authenticator {
		return &BasicAuthenticator{
			Username: username,
			Password: password,
		}
	}
}

// WithCookieAuth configures the request to use Cookie Authentication.
// Typically used with the AuthSession cookie from SessionService.Login.
func WithCookieAuth(cookie *http.Cookie) RequestOption {
	return func() Authenticator {
		return &CookieAuthenticator{
			Cookie: cookie,
		}
	}
}

// WithProxyAuth configures the request to use Proxy Authentication.
// This requires CouchDB to be configured with proxy authentication enabled.
func WithProxyAuth(username string, roles []string, token string) RequestOption {
	return func() Authenticator {
		return &ProxyAuthenticator{
			Username: username,
			Roles:    roles,
			Token:    token,
		}
	}
}

// WithJWTAuth configures the request to use JWT Bearer Token Authentication.
// This requires CouchDB to be configured with JWT authentication enabled.
func WithJWTAuth(token string) RequestOption {
	return func() Authenticator {
		return &JWTAuthenticator{
			Token: token,
		}
	}
}

// NewClient creates a new CouchDB client with the given options.
// Example usage:
//
//	client := NewClient("http://localhost:5984")
//	client := NewClient("http://localhost:5984", WithHTTPClient(customClient))
func NewClient(baseURL string, opts ...ClientOption) *Client {
	client := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  http.DefaultClient,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

// ErrorResponse represents an error response from CouchDB.
type ErrorResponse struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
}

// doRequest performs an HTTP request with optional authentication.
func (c *Client) doRequest(ctx context.Context, method, path string, body io.Reader, opts ...RequestOption) (*http.Response, error) {
	reqURL := fmt.Sprintf("%s%s", c.baseURL, path)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, err
	}

	// Apply authentication if provided.
	if len(opts) > 0 {
		// Use the last auth option if multiple are provided.
		authenticator := opts[len(opts)-1]()
		if err := authenticator.Authenticate(req); err != nil {
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	req.Header.Set("Content-Type", "application/json")

	return c.client.Do(req)
}

// Configuration returns the ConfigurationService.
func (c *Client) Configuration() *ConfigurationService {
	return &ConfigurationService{client: c}
}

// Databases returns the DatabaseService.
func (c *Client) Databases() *DatabaseService {
	return &DatabaseService{client: c}
}

// DesignDocuments returns the DesignDocumentService.
func (c *Client) DesignDocuments() *DesignDocumentService {
	return &DesignDocumentService{client: c}
}

// Documents returns the DocumentService.
func (c *Client) Documents() *DocumentService {
	return &DocumentService{client: c}
}

// Security returns the SecurityService.
func (c *Client) Security() *SecurityService {
	return &SecurityService{client: c}
}

// Server returns the ServerService.
func (c *Client) Server() *ServerService {
	return &ServerService{client: c}
}

// Sessions returns the SessionService.
func (c *Client) Sessions() *SessionService {
	return &SessionService{client: c}
}

// Users returns the UserService.
func (c *Client) Users() *UserService {
	return &UserService{client: c}
}

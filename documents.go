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

// DocumentService provides methods for managing CouchDB documents.
type DocumentService struct {
	client *Client
}

// NewDocumentService creates a new DocumentService.
func NewDocumentService(client *Client) *DocumentService {
	return &DocumentService{client: client}
}

// Document represents a CouchDB document.
type Document struct {
	ID  string `json:"_id,omitempty"`
	Rev string `json:"_rev,omitempty"`
}

// DocumentResponse represents the response from document operations.
type DocumentResponse struct {
	OK  bool   `json:"ok"`
	ID  string `json:"id,omitempty"`
	Rev string `json:"rev,omitempty"`
}

// DocumentGetOptions represents options for getting a document.
type DocumentGetOptions struct {
	Rev              string   `url:"rev,omitempty"`
	Revs             bool     `url:"revs,omitempty"`
	RevsInfo         bool     `url:"revs_info,omitempty"`
	OpenRevs         []string `url:"-"` // Special handling needed
	Latest           bool     `url:"latest,omitempty"`
	Conflicts        bool     `url:"conflicts,omitempty"`
	DeletedConflicts bool     `url:"deleted_conflicts,omitempty"`
	LocalSeq         bool     `url:"local_seq,omitempty"`
	Meta             bool     `url:"meta,omitempty"`
}

// DocumentPutOptions represents options for creating/updating a document.
type DocumentPutOptions struct {
	Rev   string `url:"rev,omitempty"`
	Batch string `url:"batch,omitempty"` // "ok" for batch mode
}

// GetDocument retrieves a document from a database.
func (s *DocumentService) GetDocument(ctx context.Context, dbName, docID string, options *DocumentGetOptions, opts ...RequestOption) (map[string]any, error) {
	path := fmt.Sprintf("/%s/%s", url.PathEscape(dbName), url.PathEscape(docID))

	// Add query parameters if options provided
	if options != nil {
		query := url.Values{}
		if options.Rev != "" {
			query.Set("rev", options.Rev)
		}
		if options.Revs {
			query.Set("revs", "true")
		}
		if options.RevsInfo {
			query.Set("revs_info", "true")
		}
		if options.Latest {
			query.Set("latest", "true")
		}
		if options.Conflicts {
			query.Set("conflicts", "true")
		}
		if options.DeletedConflicts {
			query.Set("deleted_conflicts", "true")
		}
		if options.LocalSeq {
			query.Set("local_seq", "true")
		}
		if options.Meta {
			query.Set("meta", "true")
		}
		if len(query) > 0 {
			path = fmt.Sprintf("%s?%s", path, query.Encode())
		}
	}

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get document: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("document not found: %s/%s", dbName, docID)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to get document: %s - %s", errResp.Error, errResp.Reason)
	}

	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}

	return doc, nil
}

// HeadDocument checks if a document exists and returns its revision.
func (s *DocumentService) HeadDocument(ctx context.Context, dbName, docID string, options *DocumentGetOptions, opts ...RequestOption) (string, error) {
	path := fmt.Sprintf("/%s/%s", url.PathEscape(dbName), url.PathEscape(docID))

	// Add query parameters if options provided
	if options != nil && options.Rev != "" {
		path = fmt.Sprintf("%s?rev=%s", path, url.QueryEscape(options.Rev))
	}

	resp, err := s.client.doRequest(ctx, http.MethodHead, path, nil, opts...)
	if err != nil {
		return "", fmt.Errorf("failed to head document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("document not found: %s/%s", dbName, docID)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("request failed with status %d", resp.StatusCode)
	}

	// Get ETag header which contains the revision.
	etag := resp.Header.Get("ETag")
	if etag != "" {
		// Remove quotes from ETag
		if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
			etag = etag[1 : len(etag)-1]
		}
	}

	return etag, nil
}

// CreateDocument creates a new document in a database.
func (s *DocumentService) CreateDocument(ctx context.Context, dbName string, doc any, options *DocumentPutOptions, opts ...RequestOption) (*DocumentResponse, error) {
	path := fmt.Sprintf("/%s", url.PathEscape(dbName))

	// Add query parameters if options provided
	if options != nil {
		query := url.Values{}
		if options.Batch != "" {
			query.Set("batch", options.Batch)
		}
		if len(query) > 0 {
			path = fmt.Sprintf("%s?%s", path, query.Encode())
		}
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPost, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create document: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to create document: %s - %s", errResp.Error, errResp.Reason)
	}

	var docResp DocumentResponse
	if err := json.Unmarshal(body, &docResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &docResp, nil
}

// UpdateDocument updates an existing document in a database.
func (s *DocumentService) UpdateDocument(ctx context.Context, dbName, docID string, doc any, options *DocumentPutOptions, opts ...RequestOption) (*DocumentResponse, error) {
	path := fmt.Sprintf("/%s/%s", url.PathEscape(dbName), url.PathEscape(docID))

	// Add query parameters if options provided
	if options != nil {
		query := url.Values{}
		if options.Rev != "" {
			query.Set("rev", options.Rev)
		}
		if options.Batch != "" {
			query.Set("batch", options.Batch)
		}
		if len(query) > 0 {
			path = fmt.Sprintf("%s?%s", path, query.Encode())
		}
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to update document: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to update document: %s - %s", errResp.Error, errResp.Reason)
	}

	var docResp DocumentResponse
	if err := json.Unmarshal(body, &docResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &docResp, nil
}

// DeleteDocument deletes a document from a database.
func (s *DocumentService) DeleteDocument(ctx context.Context, dbName, docID string, rev string, opts ...RequestOption) (*DocumentResponse, error) {
	path := fmt.Sprintf("/%s/%s?rev=%s", url.PathEscape(dbName), url.PathEscape(docID), url.QueryEscape(rev))

	resp, err := s.client.doRequest(ctx, http.MethodDelete, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to delete document: %w", err)
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
		return nil, fmt.Errorf("failed to delete document: %s - %s", errResp.Error, errResp.Reason)
	}

	var docResp DocumentResponse
	if err := json.Unmarshal(body, &docResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &docResp, nil
}

package couchdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// DesignDocumentService provides methods for working with design documents and views.
type DesignDocumentService struct {
	client *Client
}

// NewDesignDocumentService creates a new DesignDocumentService.
func NewDesignDocumentService(client *Client) *DesignDocumentService {
	return &DesignDocumentService{client: client}
}

// ViewOptions represents options for querying a view.
type ViewOptions struct {
	Conflicts     bool   `url:"conflicts,omitempty"`
	Descending    bool   `url:"descending,omitempty"`
	EndKey        any    `url:"-"` // JSON encoded
	EndKeyDocID   string `url:"endkey_docid,omitempty"`
	Group         bool   `url:"group,omitempty"`
	GroupLevel    int    `url:"group_level,omitempty"`
	IncludeDocs   bool   `url:"include_docs,omitempty"`
	InclusiveEnd  bool   `url:"inclusive_end,omitempty"`
	Key           any    `url:"-"` // JSON encoded
	Keys          []any  `url:"-"` // POST body
	Limit         int    `url:"limit,omitempty"`
	Reduce        *bool  `url:"-"` // Special handling - explicit true/false
	Skip          int    `url:"skip,omitempty"`
	Sorted        bool   `url:"sorted,omitempty"`
	Stable        bool   `url:"stable,omitempty"`
	Stale         string `url:"stale,omitempty"`
	StartKey      any    `url:"-"` // JSON encoded
	StartKeyDocID string `url:"startkey_docid,omitempty"`
	Update        string `url:"update,omitempty"`
	UpdateSeq     bool   `url:"update_seq,omitempty"`
}

// ViewRow represents a single row in a view response.
type ViewRow struct {
	ID    string         `json:"id"`
	Key   any            `json:"key"`
	Value any            `json:"value"`
	Doc   map[string]any `json:"doc,omitempty"`
}

// ViewResponse represents the response from a view query.
type ViewResponse struct {
	Offset    int       `json:"offset"`
	TotalRows int       `json:"total_rows"`
	Rows      []ViewRow `json:"rows"`
	UpdateSeq string    `json:"update_seq,omitempty"`
}

// QueryView queries a design document view.
func (s *DesignDocumentService) QueryView(ctx context.Context, dbName, ddoc, viewName string, options *ViewOptions, opts ...RequestOption) (*ViewResponse, error) {
	path := fmt.Sprintf("/%s/_design/%s/_view/%s",
		url.PathEscape(dbName),
		url.PathEscape(ddoc),
		url.PathEscape(viewName))

	// Build query parameters
	if options != nil {
		query := url.Values{}

		if options.Conflicts {
			query.Set("conflicts", "true")
		}
		if options.Descending {
			query.Set("descending", "true")
		}
		if options.EndKey != nil {
			endKeyJSON, _ := json.Marshal(options.EndKey)
			query.Set("endkey", string(endKeyJSON))
		}
		if options.EndKeyDocID != "" {
			query.Set("endkey_docid", options.EndKeyDocID)
		}
		if options.Group {
			query.Set("group", "true")
		}
		if options.GroupLevel > 0 {
			query.Set("group_level", fmt.Sprintf("%d", options.GroupLevel))
		}
		if options.IncludeDocs {
			query.Set("include_docs", "true")
		}
		if options.InclusiveEnd {
			query.Set("inclusive_end", "true")
		}
		if options.Key != nil {
			keyJSON, _ := json.Marshal(options.Key)
			query.Set("key", string(keyJSON))
		}
		if options.Limit > 0 {
			query.Set("limit", fmt.Sprintf("%d", options.Limit))
		}
		if options.Reduce != nil {
			if *options.Reduce {
				query.Set("reduce", "true")
			} else {
				query.Set("reduce", "false")
			}
		}
		if options.Skip > 0 {
			query.Set("skip", fmt.Sprintf("%d", options.Skip))
		}
		if options.Sorted {
			query.Set("sorted", "true")
		}
		if options.Stable {
			query.Set("stable", "true")
		}
		if options.Stale != "" {
			query.Set("stale", options.Stale)
		}
		if options.StartKey != nil {
			startKeyJSON, _ := json.Marshal(options.StartKey)
			query.Set("startkey", string(startKeyJSON))
		}
		if options.StartKeyDocID != "" {
			query.Set("startkey_docid", options.StartKeyDocID)
		}
		if options.Update != "" {
			query.Set("update", options.Update)
		}
		if options.UpdateSeq {
			query.Set("update_seq", "true")
		}

		if len(query) > 0 {
			path = fmt.Sprintf("%s?%s", path, query.Encode())
		}
	}

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to query view: %w", err)
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
		return nil, fmt.Errorf("failed to query view: %s - %s", errResp.Error, errResp.Reason)
	}

	var viewResp ViewResponse
	if err := json.Unmarshal(body, &viewResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &viewResp, nil
}

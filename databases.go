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

// DatabaseService provides methods for managing CouchDB databases.
type DatabaseService struct {
	client *Client
}

// NewDatabaseService creates a new DatabaseService.
func NewDatabaseService(client *Client) *DatabaseService {
	return &DatabaseService{client: client}
}

type DatabaseInfoCluster struct {
	N int `json:"n"`
	Q int `json:"q"`
	R int `json:"r"`
	W int `json:"w"`
}

type DatabaseInfoSizes struct {
	Active   int `json:"active"`
	File     int `json:"file"`
	External int `json:"external"`
}

type DatabaseInfoProps struct {
	Partitioned bool `json:"partitioned,omitempty"`
}

// DatabaseInfo represents information about a database.
type DatabaseInfo struct {
	Cluster           DatabaseInfoCluster `json:"cluster"`
	CompactRunning    bool                `json:"compact_running"`
	DBName            string              `json:"db_name"`
	DiskFormatVersion int                 `json:"disk_format_version"`
	DocCount          int                 `json:"doc_count"`
	DocDelCount       int                 `json:"doc_del_count"`
	InstanceStartTime string              `json:"instance_start_time"`
	PurgeSeq          string              `json:"purge_seq"`
	Sizes             DatabaseInfoSizes   `json:"sizes"`
	UpdateSeq         string              `json:"update_seq"`
	Props             DatabaseInfoProps   `json:"props,omitempty"`
}

// DatabaseResponse represents the response from database operations.
type DatabaseResponse struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// DatabaseCreateOptions represents options for creating a database.
type DatabaseCreateOptions struct {
	Q           int  `json:"q,omitempty"`
	N           int  `json:"n,omitempty"`
	Partitioned bool `json:"partitioned,omitempty"`
}

// GetDatabase retrieves information about a database.
func (s *DatabaseService) GetDatabase(ctx context.Context, dbName string, opts ...RequestOption) (*DatabaseInfo, error) {
	path := fmt.Sprintf("/%s", url.PathEscape(dbName))

	resp, err := s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("database not found: %s", dbName)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("failed to get database: %s - %s", errResp.Error, errResp.Reason)
	}

	var dbInfo DatabaseInfo
	if err := json.Unmarshal(body, &dbInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal database info: %w", err)
	}

	return &dbInfo, nil
}

// CreateDatabase creates a new database.
func (s *DatabaseService) CreateDatabase(ctx context.Context, dbName string, options *DatabaseCreateOptions, opts ...RequestOption) (*DatabaseResponse, error) {
	path := fmt.Sprintf("/%s", url.PathEscape(dbName))

	// Add query parameters if options provided
	if options != nil {
		query := url.Values{}
		if options.Q > 0 {
			query.Set("q", fmt.Sprintf("%d", options.Q))
		}
		if options.N > 0 {
			query.Set("n", fmt.Sprintf("%d", options.N))
		}
		if options.Partitioned {
			query.Set("partitioned", "true")
		}
		if len(query) > 0 {
			path = fmt.Sprintf("%s?%s", path, query.Encode())
		}
	}

	resp, err := s.client.doRequest(ctx, http.MethodPut, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
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
		return nil, fmt.Errorf("failed to create database: %s - %s", errResp.Error, errResp.Reason)
	}

	var dbResp DatabaseResponse
	if err := json.Unmarshal(body, &dbResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &dbResp, nil
}

// DeleteDatabase deletes a database.
func (s *DatabaseService) DeleteDatabase(ctx context.Context, dbName string, opts ...RequestOption) (*DatabaseResponse, error) {
	path := fmt.Sprintf("/%s", url.PathEscape(dbName))

	resp, err := s.client.doRequest(ctx, http.MethodDelete, path, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to delete database: %w", err)
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
		return nil, fmt.Errorf("failed to delete database: %s - %s", errResp.Error, errResp.Reason)
	}

	var dbResp DatabaseResponse
	if err := json.Unmarshal(body, &dbResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &dbResp, nil
}

// DatabaseExists checks if a database exists.
func (s *DatabaseService) DatabaseExists(ctx context.Context, dbName string, opts ...RequestOption) (bool, error) {
	path := fmt.Sprintf("/%s", url.PathEscape(dbName))

	resp, err := s.client.doRequest(ctx, http.MethodHead, path, nil, opts...)
	if err != nil {
		return false, fmt.Errorf("failed to check database: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}

	return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// BulkDocItem represents a single document in a bulk operation response.
type BulkDocItem struct {
	ID  string `json:"id"`
	OK  bool   `json:"ok"`
	Rev string `json:"rev"`
}

// BulkDocsResponse represents the response from bulk operations.
type BulkDocsResponse []BulkDocItem

// BulkInsert inserts multiple documents in a single request.
func (s *DatabaseService) BulkInsert(ctx context.Context, dbName string, docs []map[string]any, opts ...RequestOption) (BulkDocsResponse, error) {
	path := fmt.Sprintf("/%s/_bulk_docs", url.PathEscape(dbName))

	body := map[string]any{
		"docs": docs,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bulk docs: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPost, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk insert: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(respBody, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
		}
		return nil, fmt.Errorf("failed to bulk insert: %s - %s", errResp.Error, errResp.Reason)
	}

	var bulkResp BulkDocsResponse
	if err := json.Unmarshal(respBody, &bulkResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return bulkResp, nil
}

// BulkUpdate updates or deletes multiple documents in a single request.
func (s *DatabaseService) BulkUpdate(ctx context.Context, dbName string, docs []map[string]any, opts ...RequestOption) (BulkDocsResponse, error) {
	path := fmt.Sprintf("/%s/_bulk_docs", url.PathEscape(dbName))

	body := map[string]any{
		"docs": docs,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bulk docs: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPost, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk update: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(respBody, &errResp); err != nil {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
		}
		return nil, fmt.Errorf("failed to bulk update: %s - %s", errResp.Error, errResp.Reason)
	}

	var bulkResp BulkDocsResponse
	if err := json.Unmarshal(respBody, &bulkResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return bulkResp, nil
}

// FindRequest represents a Mango query request.
type FindRequest struct {
	Selector       map[string]any      `json:"selector"`
	Limit          int                 `json:"limit,omitempty"`
	Skip           int                 `json:"skip,omitempty"`
	Sort           []map[string]string `json:"sort,omitempty"`
	Fields         []string            `json:"fields,omitempty"`
	UseIndex       any                 `json:"use_index,omitempty"` // string or []string
	R              int                 `json:"r,omitempty"`
	Bookmark       string              `json:"bookmark,omitempty"`
	Update         bool                `json:"update,omitempty"`
	Stable         bool                `json:"stable,omitempty"`
	Conflicts      bool                `json:"conflicts,omitempty"`
	ExecutionStats bool                `json:"execution_stats,omitempty"`
}

// FindExecutionStats represents execution statistics for a find query.
type FindExecutionStats struct {
	TotalKeysExamined       int     `json:"total_keys_examined"`
	TotalDocsExamined       int     `json:"total_docs_examined"`
	TotalQuorumDocsExamined int     `json:"total_quorum_docs_examined"`
	ResultsReturned         int     `json:"results_returned"`
	ExecutionTimeMs         float64 `json:"execution_time_ms"`
}

// FindResponse represents the response from a find query.
type FindResponse struct {
	Docs           []map[string]any    `json:"docs"`
	Bookmark       string              `json:"bookmark"`
	ExecutionStats *FindExecutionStats `json:"execution_stats,omitempty"`
	Warning        string              `json:"warning,omitempty"`
}

// Find queries a database using Mango query language.
func (s *DatabaseService) Find(ctx context.Context, dbName string, query *FindRequest, opts ...RequestOption) (*FindResponse, error) {
	path := fmt.Sprintf("/%s/_find", url.PathEscape(dbName))

	data, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal find request: %w", err)
	}

	resp, err := s.client.doRequest(ctx, http.MethodPost, path, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute find: %w", err)
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
		return nil, fmt.Errorf("failed to execute find: %s - %s", errResp.Error, errResp.Reason)
	}

	var findResp FindResponse
	if err := json.Unmarshal(body, &findResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &findResp, nil
}

// AllDocsOptions represents options for the _all_docs endpoint.
type AllDocsOptions struct {
	Conflicts     bool     `url:"conflicts,omitempty"`
	Descending    bool     `url:"descending,omitempty"`
	EndKey        string   `url:"endkey,omitempty"`
	EndKeyDocID   string   `url:"endkey_docid,omitempty"`
	IncludeDocs   bool     `url:"include_docs,omitempty"`
	InclusiveEnd  bool     `url:"inclusive_end,omitempty"`
	Key           string   `url:"key,omitempty"`
	Keys          []string `url:"-"` // Special handling via POST
	Limit         int      `url:"limit,omitempty"`
	Skip          int      `url:"skip,omitempty"`
	StartKey      string   `url:"startkey,omitempty"`
	StartKeyDocID string   `url:"startkey_docid,omitempty"`
	UpdateSeq     bool     `url:"update_seq,omitempty"`
}

// AllDocsRow represents a single row in the all_docs response.
type AllDocsRow struct {
	ID    string         `json:"id"`
	Key   string         `json:"key"`
	Value map[string]any `json:"value"`
	Doc   map[string]any `json:"doc,omitempty"`
}

// AllDocsResponse represents the response from _all_docs.
type AllDocsResponse struct {
	Offset    int          `json:"offset"`
	Rows      []AllDocsRow `json:"rows"`
	TotalRows int          `json:"total_rows"`
	UpdateSeq string       `json:"update_seq,omitempty"`
}

// AllDocs retrieves all documents in a database.
func (s *DatabaseService) AllDocs(ctx context.Context, dbName string, options *AllDocsOptions, opts ...RequestOption) (*AllDocsResponse, error) {
	path := fmt.Sprintf("/%s/_all_docs", url.PathEscape(dbName))

	var resp *http.Response
	var err error

	// If keys are specified, use POST
	if options != nil && len(options.Keys) > 0 {
		query := url.Values{}
		if options.IncludeDocs {
			query.Set("include_docs", "true")
		}
		if len(query) > 0 {
			path = fmt.Sprintf("%s?%s", path, query.Encode())
		}

		body := map[string]any{
			"keys": options.Keys,
		}
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal keys: %w", err)
		}

		resp, err := s.client.doRequest(ctx, http.MethodPost, path, bytes.NewReader(data), opts...)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
	} else {
		// Build query parameters
		if options != nil {
			query := url.Values{}
			if options.Conflicts {
				query.Set("conflicts", "true")
			}
			if options.Descending {
				query.Set("descending", "true")
			}
			if options.EndKey != "" {
				query.Set("endkey", fmt.Sprintf(`"%s"`, options.EndKey))
			}
			if options.EndKeyDocID != "" {
				query.Set("endkey_docid", options.EndKeyDocID)
			}
			if options.IncludeDocs {
				query.Set("include_docs", "true")
			}
			if options.InclusiveEnd {
				query.Set("inclusive_end", "true")
			}
			if options.Key != "" {
				query.Set("key", fmt.Sprintf(`"%s"`, options.Key))
			}
			if options.Limit > 0 {
				query.Set("limit", fmt.Sprintf("%d", options.Limit))
			}
			if options.Skip > 0 {
				query.Set("skip", fmt.Sprintf("%d", options.Skip))
			}
			if options.StartKey != "" {
				query.Set("startkey", fmt.Sprintf(`"%s"`, options.StartKey))
			}
			if options.StartKeyDocID != "" {
				query.Set("startkey_docid", options.StartKeyDocID)
			}
			if options.UpdateSeq {
				query.Set("update_seq", "true")
			}
			if len(query) > 0 {
				path = fmt.Sprintf("%s?%s", path, query.Encode())
			}
		}

		resp, err = s.client.doRequest(ctx, http.MethodGet, path, nil, opts...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get all docs: %w", err)
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
		return nil, fmt.Errorf("failed to get all docs: %s - %s", errResp.Error, errResp.Reason)
	}

	var allDocsResp AllDocsResponse
	if err := json.Unmarshal(body, &allDocsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &allDocsResp, nil
}

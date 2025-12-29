# couchdb

[CouchDB](https://docs.couchdb.org/en/stable/api/database/index.html) client

[![Go Reference](https://pkg.go.dev/badge/github.com/tetsuo/couchdb.svg)](https://pkg.go.dev/github.com/tetsuo/couchdb)

## Usage

Install with:

```bash
go get github.com/tetsuo/couchdb
```

Then use it like this:

```go
package main

import (
	"context"
	"github.com/tetsuo/couchdb"
)

func main() {
	client := couchdb.NewClient("http://localhost:5984")

	// Create services
	dbs := couchdb.NewDatabaseService(client) // or client.Databases()
	docs := couchdb.Documents()

	ctx := context.Background()

	// Admin creates database
	dbs.CreateDatabase(ctx, "users_db", nil,
		couchdb.WithBasicAuth("admin", "adminpass"))

	// User creates document
	doc := map[string]any{
		"name":  "John Doe",
		"email": "john@example.com",
	}
	docs.CreateDocument(ctx, "users_db", doc, nil,
		couchdb.WithBasicAuth("john", "johnpass"))

	// Different user with JWT token
	docs.GetDocument(ctx, "users_db", "doc123", nil,
		couchdb.WithJWTAuth("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."))
}
```

## Authentication

Supported authentication methods:

- `WithBasicAuth(username, password string)` — HTTP Basic Auth
- `WithJWTAuth(token string)` — JWT Bearer token
- `WithCookieAuth(cookie *http.Cookie)` — Session cookie
- `WithProxyAuth(username string, roles []string, token string)` — Proxy auth

## FAQ

### Which CouchDB versions are supported?

Tested with CouchDB v3.5.x, but should work with other 3.x versions as well.

### Is every CouchDB API covered?

Not yet. If you need a specific API that is not implemented, feel free to open an issue or contribute a PR. For example, cluster and replication APIs are not yet implemented.

## Documentation

Full API documentation is at [pkg.go.dev](https://pkg.go.dev/github.com/tetsuo/couchdb).

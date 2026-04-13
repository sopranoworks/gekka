// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	httpDSL "github.com/sopranoworks/gekka/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServer_PathAndMethod — GET /hello → 200 "hello"
func TestServer_PathAndMethod(t *testing.T) {
	route := httpDSL.Path("/hello", httpDSL.Get(httpDSL.Complete(200, "hello")))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/hello")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "hello", string(body))
}

// TestServer_PathParam — GET /users/42 → 200 "user=42"
func TestServer_PathParam(t *testing.T) {
	route := httpDSL.PathPrefix("/users", httpDSL.PathParam("id", func(id string) httpDSL.Route {
		return httpDSL.Get(httpDSL.Complete(200, "user="+id))
	}))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/users/42")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "user=42", string(body))
}

// TestServer_QueryParam — GET /search?q=go → 200 "q=go"
func TestServer_QueryParam(t *testing.T) {
	route := httpDSL.Path("/search", httpDSL.Get(
		httpDSL.Parameter("q", func(q string) httpDSL.Route {
			return httpDSL.Complete(200, "q="+q)
		}),
	))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/search?q=go")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "q=go", string(body))
}

// TestServer_Header — GET /auth with X-Token header → 200 "token=<value>"
func TestServer_Header(t *testing.T) {
	route := httpDSL.Path("/auth", httpDSL.Get(
		httpDSL.Header("X-Token", func(tok string) httpDSL.Route {
			return httpDSL.Complete(200, "token="+tok)
		}),
	))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	req, err := http.NewRequest(http.MethodGet, "http://"+b.Addr+"/auth", nil)
	require.NoError(t, err)
	req.Header.Set("X-Token", "secret123")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "token=secret123", string(body))
}

// TestServer_Concat_FirstMatch — GET /ping matched before fallback → 200 "pong"
func TestServer_Concat_FirstMatch(t *testing.T) {
	route := httpDSL.Concat(
		httpDSL.Path("/ping", httpDSL.Get(httpDSL.Complete(200, "pong"))),
		httpDSL.Path("/ping", httpDSL.Get(httpDSL.Complete(200, "fallback"))),
	)
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/ping")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "pong", string(body))
}

// TestServer_404 — GET /notfound → 404
func TestServer_404(t *testing.T) {
	route := httpDSL.Path("/hello", httpDSL.Get(httpDSL.Complete(200, "hello")))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/notfound")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 404, resp.StatusCode)
}

// TestServer_405 — POST /hello (route only has GET) → 405, Allow header contains "GET"
func TestServer_405(t *testing.T) {
	route := httpDSL.Path("/hello", httpDSL.Get(httpDSL.Complete(200, "hello")))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Post("http://"+b.Addr+"/hello", "text/plain", strings.NewReader("body"))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 405, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Allow"), "GET")
}

// TestServer_NestedPathPrefix — GET /api/v1/status → 200 "ok"
func TestServer_NestedPathPrefix(t *testing.T) {
	route := httpDSL.PathPrefix("/api",
		httpDSL.PathPrefix("/v1",
			httpDSL.Path("/status",
				httpDSL.Get(httpDSL.Complete(200, "ok")),
			),
		),
	)
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/api/v1/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "ok", string(body))
}

// TestServer_CompleteJSON — GET /json → 200 Content-Type application/json, body {"ok":true}
func TestServer_CompleteJSON(t *testing.T) {
	type payload struct {
		Ok bool `json:"ok"`
	}
	route := httpDSL.Path("/json", httpDSL.Get(httpDSL.Complete(200, payload{Ok: true})))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	resp, err := http.Get("http://" + b.Addr + "/json")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	body, _ := io.ReadAll(resp.Body)
	// json.Encoder appends a newline
	assert.Equal(t, `{"ok":true}`+"\n", string(body))
}

// TestServer_Redirect — GET /old → 302, Location "/new"
func TestServer_Redirect(t *testing.T) {
	route := httpDSL.Path("/old", httpDSL.Get(httpDSL.Redirect("/new", 302)))
	b, err := httpDSL.NewServer(":0", route)
	require.NoError(t, err)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	// Use a client that does NOT follow redirects
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get("http://" + b.Addr + "/old")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 302, resp.StatusCode)
	assert.Equal(t, "/new", resp.Header.Get("Location"))
}

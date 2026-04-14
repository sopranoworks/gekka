// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

// Package testkit provides RouteTestKit for in-process HTTP route testing
// without starting a live server, matching Pekko HTTP's RouteTest pattern.
package testkit

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ghttp "github.com/sopranoworks/gekka/http"
)

// TestResponse holds the result of a synthetic test request.
type TestResponse struct {
	StatusCode int
	Body       string
	Headers    http.Header
}

// RouteTestKit enables in-process route testing without a live server.
type RouteTestKit struct {
	t       *testing.T
	handler http.Handler
}

// New creates a RouteTestKit wrapping route.
// t is used to report request construction failures immediately.
func New(t *testing.T, route ghttp.Route) *RouteTestKit {
	t.Helper()
	return &RouteTestKit{t: t, handler: ghttp.ToHandler(route)}
}

// Get sends a synthetic GET request to path.
func (k *RouteTestKit) Get(path string) TestResponse {
	return k.do("GET", path, "", nil)
}

// Post sends a synthetic POST with body to path.
func (k *RouteTestKit) Post(path, contentType string, body []byte) TestResponse {
	return k.do("POST", path, contentType, body)
}

// Put sends a synthetic PUT with body to path.
func (k *RouteTestKit) Put(path, contentType string, body []byte) TestResponse {
	return k.do("PUT", path, contentType, body)
}

// Delete sends a synthetic DELETE to path.
func (k *RouteTestKit) Delete(path string) TestResponse {
	return k.do("DELETE", path, "", nil)
}

// Request sends a fully custom synthetic request with extra headers.
func (k *RouteTestKit) Request(method, path, contentType string, body []byte, extraHeaders map[string]string) TestResponse {
	req := k.buildReq(method, path, contentType, body)
	for key, val := range extraHeaders {
		req.Header.Set(key, val)
	}
	return k.run(req)
}

func (k *RouteTestKit) do(method, path, contentType string, body []byte) TestResponse {
	return k.run(k.buildReq(method, path, contentType, body))
}

func (k *RouteTestKit) buildReq(method, path, contentType string, body []byte) *http.Request {
	req, err := http.NewRequest(method, path, bytes.NewReader(body))
	if err != nil {
		k.t.Helper()
		k.t.Fatalf("testkit: failed to build request %s %s: %v", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req
}

func (k *RouteTestKit) run(req *http.Request) TestResponse {
	rec := httptest.NewRecorder()
	k.handler.ServeHTTP(rec, req)
	return TestResponse{
		StatusCode: rec.Code,
		Body:       strings.TrimSuffix(rec.Body.String(), "\n"),
		Headers:    rec.Header(),
	}
}

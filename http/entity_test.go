// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	ghttp "github.com/sopranoworks/gekka/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type helloBody struct {
	Name string `json:"name"`
}

// roundTrip starts a temporary server with route, sends one HTTP request,
// and returns the response. The caller must close resp.Body.
func roundTrip(t *testing.T, method, path string, body io.Reader, contentType string, route ghttp.Route) *http.Response {
	t.Helper()
	srv, err := ghttp.NewServer(":0", route)
	require.NoError(t, err)
	defer srv.Shutdown(context.Background()) //nolint:errcheck

	req, err := http.NewRequest(method, "http://"+srv.Addr+path, body)
	require.NoError(t, err)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestEntity_JSONDecode(t *testing.T) {
	body, _ := json.Marshal(helloBody{Name: "world"})
	route := ghttp.Post(
		ghttp.Entity[helloBody](func(h helloBody) ghttp.Route {
			return ghttp.Complete(200, map[string]string{"greeting": "hello " + h.Name})
		}),
	)
	resp := roundTrip(t, "POST", "/", bytes.NewReader(body), "application/json", route)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
}

func TestEntity_BadJSON(t *testing.T) {
	route := ghttp.Post(
		ghttp.Entity[helloBody](func(h helloBody) ghttp.Route {
			return ghttp.Complete(200, nil)
		}),
	)
	resp := roundTrip(t, "POST", "/", strings.NewReader("not-json"), "application/json", route)
	defer resp.Body.Close()
	assert.Equal(t, 400, resp.StatusCode)
}

func TestFormValue(t *testing.T) {
	form := url.Values{"username": {"alice"}}
	route := ghttp.Post(
		ghttp.FormValue("username", func(u string) ghttp.Route {
			return ghttp.Complete(200, u)
		}),
	)
	resp := roundTrip(t, "POST", "/", strings.NewReader(form.Encode()),
		"application/x-www-form-urlencoded", route)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
}

func TestFormValues(t *testing.T) {
	form := url.Values{"a": {"1"}, "b": {"2"}}
	route := ghttp.Post(
		ghttp.FormValues(func(vals url.Values) ghttp.Route {
			return ghttp.Complete(200, vals.Get("a")+vals.Get("b"))
		}),
	)
	resp := roundTrip(t, "POST", "/", strings.NewReader(form.Encode()),
		"application/x-www-form-urlencoded", route)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
}

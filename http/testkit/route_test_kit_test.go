// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package testkit_test

import (
	"io"
	"testing"

	ghttp "github.com/sopranoworks/gekka/http"
	"github.com/sopranoworks/gekka/http/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func greetRoute() ghttp.Route {
	return ghttp.Concat(
		ghttp.Path("/hello", ghttp.Get(
			ghttp.Complete(200, "hello world"),
		)),
		ghttp.Path("/echo", ghttp.Post(
			func(ctx *ghttp.RequestContext) {
				body, _ := io.ReadAll(ctx.Request.Body)
				ghttp.Complete(200, string(body))(ctx)
			},
		)),
	)
}

func TestRouteTestKit_Get200(t *testing.T) {
	kit := testkit.New(t, greetRoute())
	resp := kit.Get("/hello")
	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "hello world", resp.Body)
}

func TestRouteTestKit_Post200(t *testing.T) {
	kit := testkit.New(t, greetRoute())
	resp := kit.Post("/echo", "text/plain", []byte("pekko"))
	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "pekko", resp.Body)
}

func TestRouteTestKit_NotFound(t *testing.T) {
	kit := testkit.New(t, greetRoute())
	resp := kit.Get("/missing")
	require.Equal(t, 404, resp.StatusCode)
}

func TestRouteTestKit_CustomHeaders(t *testing.T) {
	route := ghttp.Path("/auth", ghttp.Get(
		ghttp.Header("X-Token", func(v string) ghttp.Route {
			return ghttp.Complete(200, "token:"+v)
		}),
	))
	kit := testkit.New(t, route)
	resp := kit.Request("GET", "/auth", "", nil, map[string]string{"X-Token": "secret"})
	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "token:secret", resp.Body)
}

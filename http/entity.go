// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"encoding/json"
	"net/url"
)

// Entity decodes the request body as JSON into T, then calls inner with the
// decoded value. Responds 400 Bad Request if decoding fails.
func Entity[T any](inner func(T) Route) Route {
	return func(ctx *RequestContext) {
		var v T
		dec := json.NewDecoder(ctx.Request.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&v); err != nil {
			ctx.Completed = true
			ctx.Writer.WriteHeader(400)
			return
		}
		inner(v)(ctx)
	}
}

// FormValue parses the request form body and extracts field name.
// Rejects with 400 if form parsing fails; rejects (no match) if field is absent.
func FormValue(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		if err := ctx.Request.ParseForm(); err != nil {
			ctx.Completed = true
			ctx.Writer.WriteHeader(400)
			return
		}
		if _, ok := ctx.Request.Form[name]; !ok {
			ctx.Rejected = true
			return
		}
		inner(ctx.Request.FormValue(name))(ctx)
	}
}

// FormValues parses the full form and passes all values to inner.
func FormValues(inner func(url.Values) Route) Route {
	return func(ctx *RequestContext) {
		if err := ctx.Request.ParseForm(); err != nil {
			ctx.Completed = true
			ctx.Writer.WriteHeader(400)
			return
		}
		inner(ctx.Request.Form)(ctx)
	}
}

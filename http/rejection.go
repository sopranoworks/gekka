// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"net/http"
	"strings"
)

// handleRejection is the default rejection handler applied by ToHandler.
func handleRejection(ctx *RequestContext) {
	if len(ctx.AllowedMethods) > 0 {
		ctx.Writer.Header().Set("Allow", strings.Join(ctx.AllowedMethods, ", "))
		ctx.Writer.WriteHeader(http.StatusMethodNotAllowed)
	} else {
		ctx.Writer.WriteHeader(http.StatusNotFound)
	}
}

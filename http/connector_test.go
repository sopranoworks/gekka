// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	ghttp "github.com/sopranoworks/gekka/http"
	"github.com/sopranoworks/gekka/stream"
)

func TestHTTPConnector_Flow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	flow := ghttp.NewClientFlow(ghttp.ClientConfig{}, 2)

	var gotStatus int
	_, err = stream.Via(stream.FromSlice([]*http.Request{req}), flow).
		To(stream.Foreach(func(resp *http.Response) {
			gotStatus = resp.StatusCode
			resp.Body.Close()
		})).
		Run(stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("stream run error: %v", err)
	}
	if gotStatus != http.StatusOK {
		t.Errorf("expected status 200, got %d", gotStatus)
	}
}

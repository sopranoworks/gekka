// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"testing"

	ghttp "github.com/sopranoworks/gekka/http"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileUpload_StreamingRead(t *testing.T) {
	route := ghttp.Path("/upload", ghttp.Post(
		ghttp.FileUpload("file", func(filename string, body stream.Source[[]byte, stream.NotUsed], ctx *ghttp.RequestContext) {
			chunks, err := stream.RunWith(body, stream.Collect[[]byte](), stream.SyncMaterializer{})
			if err != nil {
				http.Error(ctx.Writer, err.Error(), 500)
				ctx.Completed = true
				return
			}
			var combined []byte
			for _, c := range chunks {
				combined = append(combined, c...)
			}
			ghttp.Complete(200, "file:"+filename+":"+string(combined))(ctx)
		}),
	))
	srv, err := ghttp.NewServer(":0", route)
	require.NoError(t, err)
	defer srv.Shutdown(context.Background()) //nolint:errcheck

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormFile("file", "hello.txt")
	_, _ = fw.Write([]byte("hello gekka"))
	w.Close()

	resp, err := http.Post("http://"+srv.Addr+"/upload", w.FormDataContentType(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(body[:n]), "file:hello.txt:hello gekka")
}

func TestFormDataField_TextValue(t *testing.T) {
	route := ghttp.Path("/form", ghttp.Post(
		ghttp.FormDataField("username", func(value string, ctx *ghttp.RequestContext) {
			ghttp.Complete(200, "user:"+value)(ctx)
		}),
	))
	srv, err := ghttp.NewServer(":0", route)
	require.NoError(t, err)
	defer srv.Shutdown(context.Background()) //nolint:errcheck

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("username", "alice")
	w.Close()

	resp, err := http.Post("http://"+srv.Addr+"/form", w.FormDataContentType(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(body[:n]), "user:alice")
}

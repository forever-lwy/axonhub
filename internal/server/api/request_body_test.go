// 职责: 回归验证大请求的 Gemini 转发、协议错误和项目失败记录。
// 关联文件:
//   internal/server/api/request_body.go
//   internal/server/api/gemini.go
//   internal/server/middleware/trace.go
//   internal/server/biz/request.go

package api

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/middleware"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/internal/tracing"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/transformer"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
	"github.com/looplj/axonhub/llm/transformer/gemini"
	"github.com/looplj/axonhub/llm/transformer/openai"
	"github.com/looplj/axonhub/llm/transformer/openai/responses"
)

func TestOversizedGeminiRequest(t *testing.T) {
	const limit = 128
	const path = "/v1beta/models/gpt-test:streamGenerateContent"
	for _, mode := range []string{"content-length", "chunked", "gzip"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newStreamDeliveryE2EFixture(t, llm.APIFormatOpenAIChatCompletion, "", false)
			h := fixture.handler(t)
			h.ChatCompletionOrchestrator.Inbound = gemini.NewInboundTransformer()
			engine := gin.New()
			projectID, _ := contexts.GetProjectID(fixture.ctx)
			// Exercise the same scoped insert as a normal authenticated model API request.
			engine.Use(func(c *gin.Context) {
				ctx := ent.NewContext(c.Request.Context(), fixture.client)
				ctx = contexts.WithProjectID(ctx, projectID)
				ctx = contexts.WithAPIKey(ctx, &ent.APIKey{ID: 42, ProjectID: projectID, Scopes: []string{"write_requests"}})
				c.Request = c.Request.WithContext(ctx)
				c.Next()
			})
			engine.Use(middleware.WithLoggingTracing(tracing.Config{}), middleware.WithLLMRequestBodyLimit(limit))
			engine.Use(middleware.WithTrace(tracing.Config{ExtraTraceBodyFields: []string{"trace_id"}}, nil))
			engine.POST("/v1beta/models/*action", (&GeminiHandlers{ChatCompletionHandlers: h}).GenerateContent)

			raw := []byte(strings.Repeat("private-image-data", 10))
			if mode == "gzip" {
				var buf bytes.Buffer
				writer := gzip.NewWriter(&buf)
				_, err := writer.Write(raw)
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				raw = buf.Bytes()
				require.Less(t, len(raw), limit)
			}
			req := httptest.NewRequest(http.MethodPost, path+"?alt=sse&key=private-query", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			if mode == "chunked" {
				req.ContentLength = -1
			}
			if mode == "gzip" {
				req.Header.Set("Content-Encoding", "gzip")
			}
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
			require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
			require.Equal(t, int64(413), gjson.Get(w.Body.String(), "error.code").Int())
			require.Equal(t, "INVALID_ARGUMENT", gjson.Get(w.Body.String(), "error.status").String())
			require.Contains(t, gjson.Get(w.Body.String(), "error.message").String(), "128")
			row, err := fixture.client.Request.Query().Only(fixture.ctx)
			require.NoError(t, err)
			require.Equal(t, projectID, row.ProjectID)
			require.Equal(t, 42, row.APIKeyID)
			require.Equal(t, request.StatusFailed, row.Status)
			require.Equal(t, "gpt-test", row.ModelID)
			require.Equal(t, llm.APIFormatGeminiContents.String(), row.Format)
			require.True(t, row.Stream)
			stored, err := h.ChatCompletionOrchestrator.RequestService.LoadRequestBody(fixture.ctx, row)
			require.NoError(t, err)
			require.Equal(t, "request_body_too_large", gjson.GetBytes(stored, "omitted").String())
			require.Equal(t, int64(limit), gjson.GetBytes(stored, "limit_bytes").Int())
			require.Equal(t, mode != "chunked", gjson.GetBytes(stored, "content_length").Exists())
			require.NotEmpty(t, w.Header().Get("AH-Request-Id"))
			require.Equal(t, w.Header().Get("AH-Request-Id"), gjson.GetBytes(stored, "request_id").String())
			require.NotContains(t, string(stored), "private")
			require.Zero(t, fixture.client.RequestExecution.Query().CountX(fixture.ctx))
			require.Zero(t, fixture.client.UsageLog.Query().CountX(fixture.ctx))
		})
	}
}

func TestModelRequestBodyLimitProtocols(t *testing.T) {
	for _, tc := range []struct {
		path    string
		inbound transformer.Inbound
		field   string
		want    string
	}{
		{"/v1/chat/completions", openai.NewInboundTransformer(), "error.code", "request_body_too_large"},
		{"/v1/responses", responses.NewInboundTransformer(), "error.code", "request_body_too_large"},
		{"/anthropic/v1/messages", anthropic.NewInboundTransformer(), "error.type", "request_too_large"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			h := NewChatCompletionHandlers(&orchestrator.ChatCompletionOrchestrator{
				Inbound: tc.inbound, RequestService: &biz.RequestService{},
			})
			engine := gin.New()
			engine.Use(middleware.WithLLMRequestBodyLimit(8))
			engine.Use(middleware.WithTrace(tracing.Config{ClaudeCodeTraceEnabled: true}, nil))
			engine.POST(tc.path, h.ChatCompletion)
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader("oversized"))
			req.ContentLength = -1
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
			require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
			require.Equal(t, tc.want, gjson.Get(w.Body.String(), tc.field).String())
		})
	}
}

func TestGeminiRequestAboveLegacyLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("exercises a 65.27 MiB HTTP body")
	}
	fixture := newStreamDeliveryE2EFixture(t, llm.APIFormatOpenAIChatCompletion, streamDeliveryChat(12, 34, true), false)
	h := fixture.handler(t)
	h.ChatCompletionOrchestrator.Inbound = gemini.NewInboundTransformer()
	engine := gin.New()
	engine.Use(fixture.testContext)
	engine.POST("/v1beta/models/*action", (&GeminiHandlers{ChatCompletionHandlers: h}).GenerateContent)

	// Reuse one synthetic PNG to match the incident's 189 inline images without a binary fixture.
	var preview bytes.Buffer
	encoder := png.Encoder{CompressionLevel: png.NoCompression}
	require.NoError(t, encoder.Encode(&preview, image.NewGray(image.Rect(0, 0, 512, 528))))
	const prefix = `{"contents":[{"role":"user","parts":[{"text":"hello"}`
	const suffix = `]}]}`
	const imageCount, size = 189, 68_440_556
	part := `,{"inlineData":{"mimeType":"image/png","data":"` + base64.StdEncoding.EncodeToString(preview.Bytes()) + `"}}`
	readers := []io.Reader{strings.NewReader(prefix)}
	for range imageCount {
		readers = append(readers, strings.NewReader(part))
	}
	padding := size - len(prefix) - imageCount*len(part) - len(suffix)
	require.GreaterOrEqual(t, padding, 0)
	readers = append(readers, strings.NewReader(suffix), strings.NewReader(strings.Repeat(" ", padding)))
	req := httptest.NewRequest(http.MethodPost, "/v1beta/models/gpt-test:streamGenerateContent?alt=sse", io.MultiReader(readers...))
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "STOP")
	fixture.assertDatabase(t, request.StatusCompleted, true)
}

// 职责: 验证 SSE 准备阶段的启用条件、错误回退、取消清理和跨阶段心跳时限。
// 关联文件:
//
//	internal/server/api/sse_preparation.go
//	internal/server/api/chat.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项:
//
//	阻塞准备任务由实际心跳写入或取消信号放行，避免用睡眠推断心跳已经发送。

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
	"github.com/looplj/axonhub/llm/transformer/gemini"
	"github.com/looplj/axonhub/llm/transformer/openai"
	"github.com/looplj/axonhub/llm/transformer/openai/codex"
)

func TestSSEPreparationKeepAliveScope(t *testing.T) {
	for _, tc := range []struct {
		name    string
		format  sseHeartbeatFormat
		body    string
		path    string
		alt     string
		writer  StreamWriter
		disable bool
		nilReq  bool
		want    bool
	}{
		{name: "OpenAI SSE", format: sseHeartbeatOpenAI, body: `{"stream":true}`, want: true},
		{name: "Anthropic SSE", format: sseHeartbeatAnthropic, body: `{"stream":true}`, want: true},
		{name: "Gemini SSE", format: sseHeartbeatGemini, path: "/models/test:streamGenerateContent", alt: "sse", want: true},
		{name: "disabled", format: sseHeartbeatOpenAI, body: `{"stream":true}`, disable: true},
		{name: "nonstream", format: sseHeartbeatOpenAI, body: `{"stream":false}`},
		{name: "missing stream", format: sseHeartbeatAnthropic, body: `{}`},
		{name: "string stream", format: sseHeartbeatOpenAI, body: `{"stream":"true"}`},
		{name: "compact ignores stream", format: sseHeartbeatOpenAI, path: "/v1/responses/compact", body: `{"stream":true}`},
		{name: "speech SSE without stream flag", format: sseHeartbeatOpenAI, path: "/v1/audio/speech", body: `{"stream_format":"sse"}`, want: true},
		{name: "speech binary format", format: sseHeartbeatOpenAI, path: "/v1/audio/speech", body: `{"stream":true,"stream_format":"audio"}`},
		{name: "speech binary writer", format: sseHeartbeatOpenAI, path: "/v1/audio/speech", body: `{"stream_format":"sse"}`, writer: WriteBinaryStream},
		{name: "Gemini JSON array", format: sseHeartbeatGemini, path: "/models/test:streamGenerateContent", writer: WriteGeminiStream},
		{name: "Gemini without alt", format: sseHeartbeatGemini, path: "/models/test:streamGenerateContent"},
		{name: "Gemini nonstream", format: sseHeartbeatGemini, path: "/models/test:generateContent", alt: "sse"},
		{name: "binary writer", format: sseHeartbeatOpenAI, body: `{"stream":true}`, writer: WriteBinaryStream},
		{name: "no format", body: `{"stream":true}`},
		{name: "nil request", format: sseHeartbeatOpenAI, nilReq: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &ChatCompletionHandlers{
				sseKeepAlive:       SSEKeepAliveConfig{Enabled: !tc.disable, Interval: time.Second},
				sseHeartbeatFormat: tc.format,
				StreamWriter:       tc.writer,
			}
			request := &httpclient.Request{Path: tc.path, Body: []byte(tc.body), Query: url.Values{"alt": {tc.alt}}}
			if tc.nilReq {
				request = nil
			}
			config := h.preparationKeepAlive(request)
			require.Equal(t, tc.want, config.Enabled)
			require.Equal(t, time.Second, config.Interval)
		})
	}
}

func TestSSEPreparationMultipartAudioKeepsOriginalBody(t *testing.T) {
	for _, path := range []string{"/v1/audio/transcriptions", "/v1/audio/translations"} {
		for _, streamValue := range []string{"true", "false"} {
			t.Run(path+"/"+streamValue, func(t *testing.T) {
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				require.NoError(t, writer.WriteField("model", "audio-test"))
				file, err := writer.CreateFormFile("file", "audio.wav")
				require.NoError(t, err)
				_, err = io.WriteString(file, "audio bytes containing stream=true")
				require.NoError(t, err)
				require.NoError(t, writer.WriteField("stream", streamValue))
				require.NoError(t, writer.Close())
				request := &httpclient.Request{
					Path:    path,
					Body:    body.Bytes(),
					Headers: http.Header{"Content-Type": {writer.FormDataContentType()}},
				}
				original := append([]byte(nil), request.Body...)
				h := &ChatCompletionHandlers{sseKeepAlive: SSEKeepAliveConfig{Enabled: true, Interval: time.Second}, sseHeartbeatFormat: sseHeartbeatOpenAI}
				for range 2 {
					require.Equal(t, streamValue == "true", h.preparationKeepAlive(request).Enabled)
					require.Equal(t, original, request.Body)
				}
				form, err := multipart.NewReader(bytes.NewReader(request.Body), writer.Boundary()).ReadForm(1024)
				require.NoError(t, err)
				defer form.RemoveAll()
				require.Equal(t, []string{streamValue}, form.Value["stream"])
				require.Len(t, form.File["file"], 1)
				require.Equal(t, "audio.wav", form.File["file"][0].Filename)
			})
		}
	}
}

func TestSSEPreparationFastErrorPreservesHTTPResponse(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	h := &ChatCompletionHandlers{ChatCompletionOrchestrator: &orchestrator.ChatCompletionOrchestrator{Inbound: openai.NewInboundTransformer()}}
	want := &httpclient.Error{
		StatusCode: http.StatusTooManyRequests,
		Body:       []byte(`{"error":{"type":"rate_limit_error","message":"try later"}}`),
	}
	_, err := waitWithSSEHeartbeats(c, ctx, newSSEHeartbeatState(SSEKeepAliveConfig{Enabled: true, Interval: time.Hour}), sseHeartbeatOpenAI, cancel, func() (int, error) {
		return 0, want
	})
	require.ErrorIs(t, err, want)
	require.False(t, c.Writer.Written())
	h.writePreparationError(c, err)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, string(want.Body), w.Body.String())
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

func TestSSEPreparationSlowErrorsUseNativeProtocol(t *testing.T) {
	for _, protocol := range []string{"anthropic", "gemini", "responses"} {
		t.Run(protocol, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
			defer cancel()
			h := &ChatCompletionHandlers{ChatCompletionOrchestrator: &orchestrator.ChatCompletionOrchestrator{Inbound: openai.NewInboundTransformer()}}
			switch protocol {
			case "anthropic":
				h.sseHeartbeatFormat = sseHeartbeatAnthropic
				h.ChatCompletionOrchestrator.Inbound = anthropic.NewInboundTransformer()
			case "gemini":
				h.sseHeartbeatFormat = sseHeartbeatGemini
				h.ChatCompletionOrchestrator.Inbound = gemini.NewInboundTransformer()
			case "responses":
				h.sseHeartbeatFormat = sseHeartbeatOpenAI
				h.streamAdapterFactory = newResponsesStreamAdapter
			}
			gate := make(chan struct{})
			c.Writer = &preparationFlushWriter{ResponseWriter: c.Writer, afterFlush: func() { close(gate) }}
			heartbeat := newSSEHeartbeatState(SSEKeepAliveConfig{Enabled: true, Interval: time.Hour})
			heartbeat.next = time.Now().Add(-time.Second)
			wantErr := &llm.ResponseError{StatusCode: http.StatusServiceUnavailable, Detail: llm.ErrorDetail{
				Type: "overloaded_error", Code: "provider_overloaded", Message: "try later",
			}}
			_, err := waitWithSSEHeartbeats(c, ctx, heartbeat, h.sseHeartbeatFormat, cancel, func() (int, error) {
				select {
				case <-gate:
					return 0, wantErr
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			})
			require.ErrorIs(t, err, wantErr)
			h.writePreparationError(c, err)
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Header().Get("Content-Type"), "text/event-stream")
			var payload []byte
			for line := range strings.SplitSeq(w.Body.String(), "\n") {
				if data, ok := strings.CutPrefix(line, "data:"); ok {
					payload = []byte(strings.TrimSpace(data))
				}
			}
			require.True(t, json.Valid(payload), "missing JSON error payload: %q", w.Body.String())
			switch protocol {
			case "anthropic":
				require.Equal(t, "error", gjson.GetBytes(payload, "type").String())
				require.Equal(t, "overloaded_error", gjson.GetBytes(payload, "error.type").String())
				require.Equal(t, "try later", gjson.GetBytes(payload, "error.message").String())
				require.Equal(t, 1, strings.Count(w.Body.String(), "event:error\n"))
			case "gemini":
				require.Equal(t, int64(http.StatusServiceUnavailable), gjson.GetBytes(payload, "error.code").Int())
				require.Equal(t, "UNAVAILABLE", gjson.GetBytes(payload, "error.status").String())
				require.Equal(t, "try later", gjson.GetBytes(payload, "error.message").String())
				require.NotContains(t, w.Body.String(), "event:")
			case "responses":
				require.Equal(t, "error", gjson.GetBytes(payload, "type").String())
				require.True(t, gjson.GetBytes(payload, "sequence_number").Exists())
				require.Zero(t, gjson.GetBytes(payload, "sequence_number").Int())
				require.Equal(t, "provider_overloaded", gjson.GetBytes(payload, "code").String())
				require.Equal(t, "try later", gjson.GetBytes(payload, "message").String())
				require.Equal(t, 1, strings.Count(w.Body.String(), "event:error\n"))
			}
		})
	}
}

func TestSSEPreparationPanicReturnsError(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
	defer cancel()
	value, err := waitWithSSEHeartbeats(c, ctx, newSSEHeartbeatState(SSEKeepAliveConfig{Enabled: true, Interval: time.Hour}), sseHeartbeatOpenAI, cancel, func() (int, error) {
		panic("preparation failed")
	})
	require.EqualError(t, err, "stream preparation stopped unexpectedly")
	require.Zero(t, value)
	require.False(t, c.Writer.Written())
}

func TestSSEPreparationHeartbeatFailureCancelsAndReturnsPreparedStream(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
	defer cancel()
	writeErr := errors.New("broken pipe")
	c.Writer = &heartbeatFailingResponseWriter{ResponseWriter: c.Writer, err: writeErr, failed: make(chan struct{}, 1)}
	source := streams.SliceStream([]*httpclient.StreamEvent{})
	exited := make(chan struct{})
	heartbeat := newSSEHeartbeatState(SSEKeepAliveConfig{Enabled: true, Interval: time.Hour})
	heartbeat.next = time.Now().Add(-time.Second)
	value, err := waitWithSSEHeartbeats(c, ctx, heartbeat, sseHeartbeatOpenAI, cancel, func() (streams.Stream[*httpclient.StreamEvent], error) {
		defer close(exited)
		<-ctx.Done()
		return source, nil
	})
	require.ErrorIs(t, err, errSSEPreparationWrite)
	require.ErrorIs(t, err, writeErr)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Same(t, source, value)
	select {
	case <-exited:
	default:
		t.Fatal("preparation helper returned before its worker exited")
	}
	require.NoError(t, value.Close())
}

type preparationFlushWriter struct {
	gin.ResponseWriter

	afterFlush func()
}

func (w *preparationFlushWriter) FlushError() error {
	if err := flushResponseWriter(w.ResponseWriter); err != nil {
		return err
	}
	if w.afterFlush != nil {
		notify := w.afterFlush
		w.afterFlush = nil
		notify()
	}
	return nil
}

type preparationGatedStream struct {
	streams.Stream[*httpclient.StreamEvent]

	ctx  context.Context
	gate <-chan struct{}
}

func (s *preparationGatedStream) Next() bool {
	if s.gate != nil {
		select {
		case <-s.gate:
			s.gate = nil
		case <-s.ctx.Done():
			return false
		}
	}
	return s.Stream.Next()
}

func TestSSEPreparationResponsesPrimePreservesEventsAndFastHeaders(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "fast"
		if blocked {
			name = "blocked"
		}
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
			defer cancel()
			first := &httpclient.StreamEvent{Type: "response.created", Data: []byte(`{"type":"response.created"}`), Headers: http.Header{codex.TurnStateHeader: {"turn-test"}}}
			last := &httpclient.StreamEvent{Type: "response.completed", Data: []byte(`{"type":"response.completed"}`)}
			source := streams.SliceStream([]*httpclient.StreamEvent{first, last})
			heartbeat := newSSEHeartbeatState(SSEKeepAliveConfig{Enabled: true, Interval: time.Hour})
			if blocked {
				gate := make(chan struct{})
				c.Writer = &preparationFlushWriter{ResponseWriter: c.Writer, afterFlush: func() { close(gate) }}
				source = &preparationGatedStream{Stream: source, ctx: ctx, gate: gate}
				heartbeat.next = time.Now().Add(-time.Second)
			}
			headers := make(http.Header)
			primed, err := waitWithSSEHeartbeats(c, ctx, heartbeat, sseHeartbeatOpenAI, cancel, func() (streams.Stream[*httpclient.StreamEvent], error) {
				return primeCodexTurnStateHeader(headers, source), nil
			})
			require.NoError(t, err)
			copyCodexTurnStateHeader(c.Writer.Header(), headers)
			var got []*httpclient.StreamEvent
			for primed.Next() {
				got = append(got, primed.Current())
			}
			require.NoError(t, primed.Err())
			require.Equal(t, []*httpclient.StreamEvent{first, last}, got)
			if blocked {
				require.Equal(t, ": keep-alive\n\n", w.Body.String())
			} else {
				require.False(t, c.Writer.Written())
				c.Writer.WriteHeaderNow()
				require.Equal(t, "turn-test", w.Result().Header.Get(codex.TurnStateHeader))
				require.Empty(t, w.Body.String())
			}
		})
	}
}

func TestSSEPreparationExpiredDeadlineSurvivesStageTransition(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
	defer cancel()
	heartbeat := newSSEHeartbeatState(SSEKeepAliveConfig{Enabled: true, Interval: time.Hour})
	first, err := waitWithSSEHeartbeats(c, ctx, heartbeat, sseHeartbeatOpenAI, cancel, func() (string, error) { return "first stage", nil })
	require.NoError(t, err)
	require.Equal(t, "first stage", first)
	require.Empty(t, w.Body.String())

	heartbeat.next = time.Now().Add(-time.Second)
	gate := make(chan struct{})
	c.Writer = &preparationFlushWriter{ResponseWriter: c.Writer, afterFlush: func() { close(gate) }}
	second, err := waitWithSSEHeartbeats(c, ctx, heartbeat, sseHeartbeatOpenAI, cancel, func() (string, error) {
		select {
		case <-gate:
			return "second stage", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	require.NoError(t, err)
	require.Equal(t, "second stage", second)
	require.Equal(t, 1, strings.Count(w.Body.String(), ": keep-alive\n\n"))
	require.True(t, heartbeat.next.After(time.Now()))
}

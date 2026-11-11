// 职责: 在上游请求和首条事件准备期间发送 SSE 心跳，并处理响应提交后的准备错误。
// 关联文件:
//
//	internal/server/api/chat.go
//	internal/server/api/sse_encoder.go
//	internal/server/api/responses_stream.go
//	llm/transformer/openai/audio_inbound.go
//	internal/server/orchestrator/stream_response_headers.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项:
//
//	只有请求处理 goroutine 可以写响应；后台任务不能访问 Gin Context 或响应头。
//	心跳提交 HTTP 200 后，后续错误必须通过 SSE 返回，不能再修改 HTTP 状态。
//	取消后台任务后必须等待其退出，避免关闭流与首条事件读取并发。

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

var errSSEPreparationWrite = errors.New("failed to write SSE preparation response")

func (h *ChatCompletionHandlers) preparationKeepAlive(request *httpclient.Request) SSEKeepAliveConfig {
	config := h.sseKeepAlive
	if !config.Enabled || h.StreamWriter != nil || request == nil {
		config.Enabled = false
		return config
	}
	switch h.sseHeartbeatFormat {
	case sseHeartbeatOpenAI:
		switch {
		case strings.HasSuffix(request.Path, "/responses/compact"):
			config.Enabled = false
		case strings.HasSuffix(request.Path, "/audio/speech"):
			config.Enabled = strings.EqualFold(strings.TrimSpace(gjson.GetBytes(request.Body, "stream_format").String()), "sse")
		case strings.HasSuffix(request.Path, "/audio/transcriptions"), strings.HasSuffix(request.Path, "/audio/translations"):
			config.Enabled = multipartRequestStreams(request)
		default:
			config.Enabled = gjson.GetBytes(request.Body, "stream").Type == gjson.True
		}
	case sseHeartbeatAnthropic:
		config.Enabled = gjson.GetBytes(request.Body, "stream").Type == gjson.True
	case sseHeartbeatGemini:
		config.Enabled = config.Enabled && strings.HasSuffix(request.Path, ":streamGenerateContent") && request.Query.Get("alt") == "sse"
	default:
		config.Enabled = false
	}
	return config
}

func multipartRequestStreams(request *httpclient.Request) bool {
	mediaType, params, err := mime.ParseMediaType(request.Headers.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") || params["boundary"] == "" {
		return false
	}
	reader := multipart.NewReader(bytes.NewReader(request.Body), params["boundary"])
	for {
		part, err := reader.NextPart()
		if err != nil {
			return false
		}
		if part.FormName() == "stream" && part.FileName() == "" {
			value, err := io.ReadAll(io.LimitReader(part, 16))
			if err != nil {
				return false
			}
			// Match the audio transformer's accepted true spellings. Files are
			// skipped by NextPart without allocating another copy of their bytes.
			switch strings.ToLower(strings.TrimSpace(string(value))) {
			case "true", "1", "yes":
				return true
			default:
				return false
			}
		}
	}
}

type ssePreparationResult[T any] struct {
	value T
	err   error
}

// sseHeartbeatState carries one deadline across Process, header priming and the
// stream writer. Starting a new stage must not postpone an already-due heartbeat.
type sseHeartbeatState struct {
	config  SSEKeepAliveConfig
	next    time.Time
	headers *orchestrator.StreamResponseHeaders
	// Inbound headers can be consumed by Codex transformers during Process.
	codexSession bool
}

func newSSEHeartbeatState(config SSEKeepAliveConfig) *sseHeartbeatState {
	return &sseHeartbeatState{config: config, next: time.Now().Add(config.Interval)}
}

// forwardReadyHeaders runs only on the response-writing goroutine. The
// orchestrator holds its retry gate until the selected headers are flushed.
func (heartbeat *sseHeartbeatState) forwardReadyHeaders(c *gin.Context) error {
	if heartbeat.headers == nil {
		return nil
	}
	return heartbeat.headers.Forward(func(upstream http.Header, codexSupported bool) (bool, error) {
		return heartbeat.writeReadyHeaders(c, upstream, codexSupported)
	})
}

func (heartbeat *sseHeartbeatState) writeReadyHeaders(c *gin.Context, upstream http.Header, codexSupported bool) (bool, error) {
	forward := make(http.Header)
	copyCodexTurnStateHeader(forward, upstream)
	if codexSupported && heartbeat.codexSession {
		forward = httpclient.MergeForwardResponseHeaders(forward, upstream)
	}
	if len(forward) == 0 {
		return false, nil
	}
	if c.Writer.Written() {
		log.Warn(c.Request.Context(), "Upstream state headers arrived after SSE response was committed")
		return false, nil
	}
	maps.Copy(c.Writer.Header(), forward)
	setSSEHeaders(c)
	c.Header("Access-Control-Allow-Origin", "*")
	return true, flushSSE(c.Request.Context(), c.Writer)
}

func (heartbeat *sseHeartbeatState) writeHeartbeat(c *gin.Context, ctx context.Context, format sseHeartbeatFormat) error {
	write := func(upstream http.Header, codexSupported bool) (bool, error) {
		forwarded, err := heartbeat.writeReadyHeaders(c, upstream, codexSupported)
		if err != nil {
			return forwarded, err
		}
		setSSEHeaders(c)
		c.Header("Access-Control-Allow-Origin", "*")
		return forwarded, writeSSEHeartbeatEvent(ctx, c.Writer, format)
	}
	if heartbeat.headers != nil {
		return heartbeat.headers.Write(write)
	}
	_, err := write(nil, false)
	return err
}

func waitWithSSEHeartbeats[T any](
	c *gin.Context,
	ctx context.Context,
	heartbeat *sseHeartbeatState,
	format sseHeartbeatFormat,
	cancel context.CancelFunc,
	prepare func() (T, error),
) (T, error) {
	config := heartbeat.config
	if !config.Enabled || config.Interval <= 0 || format == sseHeartbeatNone {
		return prepare()
	}

	done := make(chan ssePreparationResult[T], 1)
	go func() {
		var result ssePreparationResult[T]
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error(ctx, "Panic while preparing SSE stream", log.Any("panic", recovered))
				result.err = errors.New("stream preparation stopped unexpectedly")
			}
			done <- result
		}()
		result.value, result.err = prepare()
	}()

	timer := time.NewTimer(time.Until(heartbeat.next))
	defer timer.Stop()
	var headerUpdates <-chan struct{}
	if heartbeat.headers != nil {
		headerUpdates = heartbeat.headers.Updates()
	}
	finish := func(result ssePreparationResult[T]) (T, error) {
		if result.err == nil && ctx.Err() == nil {
			if err := heartbeat.forwardReadyHeaders(c); err != nil {
				cancel()
				return result.value, errors.Join(errSSEPreparationWrite, err)
			}
		}
		return result.value, result.err
	}
	for {
		select {
		case result := <-done:
			return finish(result)
		case <-ctx.Done():
			cancel()
			result := <-done
			return result.value, ctx.Err()
		case <-headerUpdates:
			if ctx.Err() != nil {
				cancel()
				result := <-done
				return result.value, ctx.Err()
			}
			if err := heartbeat.forwardReadyHeaders(c); err != nil {
				cancel()
				result := <-done
				return result.value, errors.Join(errSSEPreparationWrite, err)
			}
		case <-timer.C:
			if ctx.Err() != nil {
				cancel()
				result := <-done
				return result.value, ctx.Err()
			}
			// Prefer a ready result so fast failures retain their HTTP status and
			// available upstream headers are forwarded before committing SSE.
			select {
			case result := <-done:
				return finish(result)
			default:
			}
			if err := heartbeat.writeHeartbeat(c, ctx, format); err != nil {
				log.Warn(ctx, "Failed to write SSE preparation heartbeat", log.Cause(err))
				cancel()
				result := <-done
				return result.value, errors.Join(errSSEPreparationWrite, err)
			}
			log.Info(ctx, "SSE preparation heartbeat sent", log.String("heartbeat_format", sseHeartbeatFormatName(format)))
			heartbeat.next = time.Now().Add(config.Interval)
			timer.Reset(time.Until(heartbeat.next))
		}
	}
}

func (h *ChatCompletionHandlers) writePreparationError(c *gin.Context, err error) {
	ctx := c.Request.Context()
	if errors.Is(err, errSSEPreparationWrite) || errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	if !c.Writer.Written() {
		httpErr := transformOrchestratorError(ctx, err, h.ChatCompletionOrchestrator)
		c.JSON(httpErr.StatusCode, json.RawMessage(httpErr.Body))
		return
	}

	encodeErr := StreamErrorEncoder(func(ctx context.Context, err error) (*httpclient.StreamEvent, error) {
		httpErr := transformOrchestratorError(ctx, err, h.ChatCompletionOrchestrator)
		event := "error"
		if h.sseHeartbeatFormat == sseHeartbeatGemini {
			event = ""
		}
		data := httpErr.Body
		if h.sseHeartbeatFormat == sseHeartbeatAnthropic {
			var encodeError error
			data, encodeError = sjson.SetBytes(data, "type", "error")
			if encodeError != nil {
				return nil, encodeError
			}
			if gjson.GetBytes(data, "error.type").String() == "" {
				errorType := firstNonEmpty(gjson.GetBytes(httpErr.Body, "type").String(), "api_error")
				data, encodeError = sjson.SetBytes(data, "error.type", errorType)
				if encodeError != nil {
					return nil, encodeError
				}
			}
		}
		return &httpclient.StreamEvent{Type: event, Data: data}, nil
	})
	if h.streamAdapterFactory != nil {
		_, encodeErr = h.streamAdapterFactory(ctx, streams.SliceStream([]*httpclient.StreamEvent{}), h.ChatCompletionOrchestrator.SystemService)
	}
	if writeErr := writeSSEErrorEvent(ctx, c.Writer, encodeErr, err); writeErr != nil {
		log.Warn(ctx, "Failed to write SSE preparation error", log.Cause(writeErr))
	}
}

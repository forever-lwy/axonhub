// 职责: 在首条内容预读前传递成功流的响应头，并协调响应头提交与上游重试。
// 关联文件:
//
//	internal/server/orchestrator/request_execution.go
//	internal/server/orchestrator/outbound.go
//	internal/server/orchestrator/state.go
//	internal/server/api/sse_preparation.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项:
//
//	只有成功建立的上游流可以发布响应头，HTTP 错误响应不能发布。
//	响应头提交和放弃当前尝试共用一把锁，防止客户端收到旧尝试的状态头。
//	提交状态头不会确认正文交付；提交后禁止切换尝试，以保持响应头与正文一致。
package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"sync"
)

type streamResponseHeadersKey struct{}

type pendingStreamResponseHeaders struct {
	headers        http.Header
	codexSupported bool
}

// StreamResponseHeaders hands immutable upstream metadata to the response writer.
// It is scoped to one downstream request, including its upstream retries.
type StreamResponseHeaders struct {
	mu        sync.Mutex
	pending   *pendingStreamResponseHeaders
	committed bool
	updates   chan struct{}
}

func NewStreamResponseHeaders() *StreamResponseHeaders {
	return &StreamResponseHeaders{updates: make(chan struct{}, 1)}
}

func WithStreamResponseHeaders(ctx context.Context, headers *StreamResponseHeaders) context.Context {
	return context.WithValue(ctx, streamResponseHeadersKey{}, headers)
}

func streamResponseHeadersFromContext(ctx context.Context) *StreamResponseHeaders {
	headers, _ := ctx.Value(streamResponseHeadersKey{}).(*StreamResponseHeaders)
	return headers
}

func (s *StreamResponseHeaders) Updates() <-chan struct{} { return s.updates }

func (s *StreamResponseHeaders) observe(headers http.Header, codexSupported bool) {
	if s == nil || len(headers) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed {
		return
	}
	s.pending = &pendingStreamResponseHeaders{headers: headers.Clone(), codexSupported: codexSupported}
	select {
	case s.updates <- struct{}{}:
	default:
	}
}

// Forward runs on the response-writing goroutine. write reports whether it
// committed applicable upstream headers, even when the subsequent flush fails.
func (s *StreamResponseHeaders) Forward(write func(http.Header, bool) (bool, error)) error {
	return s.write(write, false)
}

// Write serializes an initial response write with upstream metadata publication.
// Unlike Forward, it also invokes write when no headers are pending, so a first
// heartbeat cannot race with already-published state headers.
func (s *StreamResponseHeaders) Write(write func(http.Header, bool) (bool, error)) error {
	return s.write(write, true)
}

func (s *StreamResponseHeaders) write(write func(http.Header, bool) (bool, error), always bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !always && (s.pending == nil || s.committed) {
		return nil
	}
	var headers http.Header
	var codexSupported bool
	if s.pending != nil && !s.committed {
		headers = s.pending.headers
		codexSupported = s.pending.codexSupported
	}
	s.pending = nil
	committed, err := write(headers, codexSupported)
	s.committed = s.committed || committed
	return err
}

func (s *StreamResponseHeaders) retryAllowed() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.committed
}

// discardAttempt arbitrates with Forward before any retry changes the selected
// model/channel. It is also called on failures before persistence can block.
func (s *StreamResponseHeaders) discardAttempt() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed {
		return errors.New("upstream response headers already committed")
	}
	s.pending = nil
	return nil
}

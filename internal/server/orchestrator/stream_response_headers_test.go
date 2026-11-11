// 职责: 验证首字前响应头传递的复制隔离、错误过滤和提交与重试之间的仲裁。
// 关联文件:
//
//	internal/server/orchestrator/stream_response_headers.go
//	internal/server/orchestrator/request_execution.go
//	internal/server/orchestrator/outbound.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项:
//
//	发布响应头不等于提交；只有下游提交成功或已提交后刷新失败才锁定上游尝试。
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
)

func TestStreamResponseHeadersCopiesMetadataAndKeepsUncommittedRetryable(t *testing.T) {
	headers := NewStreamResponseHeaders()
	source := http.Header{"X-Codex-Turn-State": {"initial"}, "X-Test": {"one", "two"}}
	headers.observe(source, true)
	source.Set("X-Codex-Turn-State", "mutated")
	source["X-Test"][0] = "changed"

	select {
	case <-headers.Updates():
	default:
		t.Fatal("published response headers did not notify the writer")
	}
	require.True(t, headers.retryAllowed(), "publication alone must not lock the attempt")
	calls := 0
	require.NoError(t, headers.Forward(func(received http.Header, codexSupported bool) (bool, error) {
		calls++
		require.True(t, codexSupported)
		require.Equal(t, "initial", received.Get("X-Codex-Turn-State"))
		require.Equal(t, []string{"one", "two"}, received["X-Test"])
		return false, nil
	}))
	require.True(t, headers.retryAllowed(), "inapplicable headers must leave retry available")
	require.NoError(t, headers.Forward(func(http.Header, bool) (bool, error) {
		t.Fatal("a consumed notification was forwarded twice")
		return false, nil
	}))
	require.Equal(t, 1, calls)
}

func TestStreamResponseHeadersHTTPErrorDoesNotPublish(t *testing.T) {
	headers := NewStreamResponseHeaders()
	state := &PersistenceState{StreamResponseHeaders: headers}
	middleware := &persistRequestExecutionMiddleware{outbound: &PersistentOutboundTransformer{state: state}}
	// Error responses pass through OnOutboundRawResponse/OnOutboundRawError,
	// while only a successful DoStream result reaches OnOutboundRawStream.
	_, err := middleware.OnOutboundRawResponse(t.Context(), &httpclient.Response{
		StatusCode: http.StatusServiceUnavailable,
		Headers:    http.Header{"X-Codex-Turn-State": {"failed-attempt"}},
	})
	require.NoError(t, err)
	middleware.OnOutboundRawError(t.Context(), errors.New("upstream rejected request"))
	require.NoError(t, headers.Forward(func(http.Header, bool) (bool, error) {
		t.Fatal("HTTP error response headers leaked to the downstream response")
		return true, nil
	}))
	require.True(t, headers.retryAllowed())
	select {
	case <-headers.Updates():
		t.Fatal("HTTP error response notified the stream header writer")
	default:
	}
}

func TestStreamResponseHeadersDiscardBeforeForwardKeepsOnlyNextAttempt(t *testing.T) {
	for _, notificationRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("notification_read_%t", notificationRead), func(t *testing.T) {
			headers := NewStreamResponseHeaders()
			headers.observe(http.Header{"X-Codex-Turn-State": {"abandoned"}}, true)
			if notificationRead {
				<-headers.Updates()
			}
			// Failure must discard a pending header before any persistence work
			// or retry preparation gives the downstream writer time to run.
			middleware := &persistRequestExecutionMiddleware{outbound: &PersistentOutboundTransformer{state: &PersistenceState{StreamResponseHeaders: headers}}}
			middleware.OnOutboundRawError(t.Context(), errors.New("first content failed"))
			require.True(t, headers.retryAllowed())
			require.NoError(t, headers.Forward(func(http.Header, bool) (bool, error) {
				t.Fatal("abandoned headers remained available after the error callback")
				return true, nil
			}))
			headers.observe(http.Header{"X-Codex-Turn-State": {"selected"}}, true)
			select {
			case <-headers.Updates():
			default:
				t.Fatal("the next attempt did not leave a ready notification")
			}
			require.NoError(t, headers.Forward(func(received http.Header, _ bool) (bool, error) {
				require.Equal(t, "selected", received.Get("X-Codex-Turn-State"))
				return true, nil
			}))
			require.False(t, headers.retryAllowed())
		})
	}
}

func TestStreamResponseHeadersCommittedAttemptRejectsBothRetryRoutes(t *testing.T) {
	for _, flushFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("flush_fails_%t", flushFails), func(t *testing.T) {
			headers := NewStreamResponseHeaders()
			candidate := &ChannelModelsCandidate{Channel: &biz.Channel{Channel: &ent.Channel{ID: 1}}, Models: []biz.ChannelModelEntry{{ActualModel: "test"}}}
			state := &PersistenceState{StreamResponseHeaders: headers, CurrentCandidate: candidate, ChannelModelsCandidates: []*ChannelModelsCandidate{candidate, candidate}}
			outbound := &PersistentOutboundTransformer{state: state}
			require.True(t, outbound.HasMoreChannels())
			require.True(t, outbound.CanRetry(pipeline.ErrEmptyResponse))
			headers.observe(http.Header{"X-Codex-Turn-State": {"committed"}}, true)
			require.True(t, outbound.HasMoreChannels(), "publication must preserve failover")
			require.True(t, outbound.CanRetry(pipeline.ErrEmptyResponse), "publication must preserve same-channel retry")
			flushErr := errors.New("flush failed after writing headers")
			err := headers.Forward(func(http.Header, bool) (bool, error) {
				if flushFails {
					return true, flushErr
				}
				return true, nil
			})
			if flushFails {
				require.ErrorIs(t, err, flushErr)
			} else {
				require.NoError(t, err)
			}
			require.False(t, outbound.HasMoreChannels())
			require.False(t, outbound.CanRetry(pipeline.ErrEmptyResponse))
			require.Error(t, outbound.NextChannel(t.Context()))
			require.Error(t, outbound.PrepareForRetry(t.Context()))
			require.Zero(t, state.CurrentCandidateIndex)
			require.Zero(t, state.CurrentModelIndex)
			require.Same(t, candidate, state.CurrentCandidate)
		})
	}
}

func TestStreamResponseHeadersForwardAndDiscardArbitrate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 64 {
		headers := NewStreamResponseHeaders()
		headers.observe(http.Header{"X-Codex-Turn-State": {"one-attempt"}}, true)
		start := make(chan struct{})
		var writes atomic.Int32
		forward := runStreamHeadersControlTask(ctx, func() error {
			<-start
			return headers.Forward(func(http.Header, bool) (bool, error) {
				writes.Add(1)
				return true, nil
			})
		})
		discard := runStreamHeadersControlTask(ctx, func() error {
			<-start
			return headers.discardAttempt()
		})
		close(start)
		require.NoError(t, waitStreamHeadersControlTask(t, ctx, forward))
		discardErr := waitStreamHeadersControlTask(t, ctx, discard)
		if discardErr == nil {
			require.Zero(t, writes.Load(), "a retried attempt must not publish stale state")
			require.True(t, headers.retryAllowed())
		} else {
			require.Equal(t, int32(1), writes.Load())
			require.False(t, headers.retryAllowed(), "a committed state header must prevent retry")
		}
	}
}

func TestStreamResponseHeadersWriteKeepsCommittedAttempt(t *testing.T) {
	headers := NewStreamResponseHeaders()
	headers.observe(http.Header{"X-Codex-Turn-State": {"selected"}}, true)
	require.NoError(t, headers.Write(func(received http.Header, supported bool) (bool, error) {
		require.True(t, supported)
		require.Equal(t, "selected", received.Get("X-Codex-Turn-State"))
		return true, nil
	}))
	require.False(t, headers.retryAllowed())
	called := false
	require.NoError(t, headers.Write(func(received http.Header, supported bool) (bool, error) {
		called = true
		require.Nil(t, received)
		require.False(t, supported)
		return false, nil
	}))
	require.True(t, called, "later heartbeat writes must still run without new metadata")
	require.False(t, headers.retryAllowed(), "an ordinary heartbeat must not unlock a selected attempt")
	require.Error(t, headers.discardAttempt())
}

func TestStreamResponseHeadersWriteAndPublicationDoNotLoseMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 64 {
		headers := NewStreamResponseHeaders()
		start := make(chan struct{})
		var included atomic.Bool
		write := runStreamHeadersControlTask(ctx, func() error {
			<-start
			return headers.Write(func(received http.Header, supported bool) (bool, error) {
				if received.Get("X-Codex-Turn-State") == "published" && supported {
					included.Store(true)
					return true, nil
				}
				return false, nil
			})
		})
		publish := runStreamHeadersControlTask(ctx, func() error {
			<-start
			headers.observe(http.Header{"X-Codex-Turn-State": {"published"}}, true)
			return nil
		})
		close(start)
		require.NoError(t, waitStreamHeadersControlTask(t, ctx, write))
		require.NoError(t, waitStreamHeadersControlTask(t, ctx, publish))
		forwarded := false
		require.NoError(t, headers.Forward(func(received http.Header, supported bool) (bool, error) {
			forwarded = true
			require.True(t, supported)
			require.Equal(t, "published", received.Get("X-Codex-Turn-State"))
			return true, nil
		}))
		// Metadata either accompanied the initial write, or arrived afterward
		// and remains pending. It cannot be silently erased between the two.
		require.NotEqual(t, included.Load(), forwarded)
		require.False(t, headers.retryAllowed())
	}
}

func runStreamHeadersControlTask(ctx context.Context, task func() error) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error(ctx, "Panic in stream header control test", log.Any("panic", recovered))
				done <- fmt.Errorf("stream header control task panicked: %v", recovered)
			}
		}()
		done <- task()
	}()
	return done
}

func waitStreamHeadersControlTask(t *testing.T, ctx context.Context, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatal("stream header arbitration did not finish")
		return ctx.Err()
	}
}

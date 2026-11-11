// 职责: 用真实 HTTP 验证首字等待心跳及时携带上游会话状态，并隔离不同重试尝试的响应头。
// 关联文件:
//
//	internal/server/api/chat.go
//	internal/server/api/sse_preparation.go
//	internal/server/orchestrator/request_execution.go
//	llm/transformer/openai/codex/headers.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项:
//
//	上游首条内容必须等客户端读到状态头和心跳后才发送，不能用正文到达后的头检查替代。
//	已发给客户端的状态头必须与输出正文属于同一次上游尝试。
package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/datastorage"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/openai/codex"
)

const sseResponseHeadersRequest = `{"model":"gpt-test","input":"hello","stream":true}`

func sseResponseHeadersPayload() string {
	return "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-headers\",\"status\":\"in_progress\",\"output\":[]}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-headers\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":34,\"total_tokens\":46}}}\n\n"
}

func TestSSEResponseHeadersHTTPBeforeFirstContent(t *testing.T) {
	for _, provider := range []channel.Type{channel.TypeCodex, channel.TypeOpenaiResponses} {
		t.Run(string(provider), func(t *testing.T) {
			bodyRelease := make(chan struct{})
			var releaseOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(bodyRelease) }) }
			defer resume()
			requests := make(chan http.Header, 2)
			var attempts atomic.Int32
			fixture := newSSEResponseHeadersHTTPFixture(t, provider, 0, 0, func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)
				requests <- r.Header.Clone()
				state := "turn-state-first"
				if attempt == 2 {
					state = "turn-state-second"
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set(codex.TurnStateHeader, state)
				w.Header().Set(httpclient.ReasoningIncludedHeader, "true")
				w.Header().Set("Set-Cookie", "provider-secret=hidden")
				w.Header().Set("X-Provider-Private", "hidden")
				w.(http.Flusher).Flush()
				if attempt == 1 {
					select {
					case <-bodyRelease:
					case <-r.Context().Done():
						return
					}
				}
				_, _ = io.WriteString(w, sseResponseHeadersPayload())
			})
			response := fixture.post(t, "")
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, "turn-state-first", response.Header.Get(codex.TurnStateHeader))
			if provider == channel.TypeCodex {
				require.Equal(t, "true", response.Header.Get(httpclient.ReasoningIncludedHeader))
			} else {
				require.Empty(t, response.Header.Get(httpclient.ReasoningIncludedHeader))
			}
			require.Empty(t, response.Header.Get("Set-Cookie"))
			require.Empty(t, response.Header.Get("X-Provider-Private"))
			assertSSEPreparationHeartbeats(t, response.Body, ": keep-alive\n\n")
			resume()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), `"delta":"hello"`)
			require.Equal(t, 1, strings.Count(string(body), "event:response.completed"))
			fixture.wait(t)
			<-requests

			// A real next request reuses the returned state under the same session.
			next := fixture.post(t, response.Header.Get(codex.TurnStateHeader))
			defer next.Body.Close()
			_, err = io.ReadAll(next.Body)
			require.NoError(t, err)
			require.Equal(t, "turn-state-second", next.Header.Get(codex.TurnStateHeader))
			fixture.wait(t)
			require.Equal(t, "turn-state-first", (<-requests).Get(codex.TurnStateHeader))
			require.Equal(t, int32(2), attempts.Load())
		})
	}
}

func TestSSEResponseHeadersHTTPFailedAttemptCannotPoisonRetry(t *testing.T) {
	bodyRelease := make(chan struct{})
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(bodyRelease) }) }
	defer resume()
	var attempts atomic.Int32
	fixture := newSSEResponseHeadersHTTPFixture(t, channel.TypeCodex, 1, 0, func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set(codex.TurnStateHeader, "poison-state-from-rejected-attempt")
			w.Header().Set(httpclient.ReasoningIncludedHeader, "false")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"message":"synthetic retryable rejection","type":"server_error"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set(codex.TurnStateHeader, "accepted-retry-state")
		w.Header().Set(httpclient.ReasoningIncludedHeader, "true")
		w.(http.Flusher).Flush()
		select {
		case <-bodyRelease:
			_, _ = io.WriteString(w, sseResponseHeadersPayload())
		case <-r.Context().Done():
		}
	})
	response := fixture.post(t, "")
	defer response.Body.Close()
	require.Equal(t, "accepted-retry-state", response.Header.Get(codex.TurnStateHeader))
	require.Equal(t, "true", response.Header.Get(httpclient.ReasoningIncludedHeader))
	assertSSEPreparationHeartbeats(t, response.Body, ": keep-alive\n\n")
	resume()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(body), "event:response.completed"))
	require.NotContains(t, string(body), "poison-state")
	require.NotContains(t, string(body), "synthetic retryable rejection")
	fixture.wait(t)
	require.Equal(t, int32(2), attempts.Load())
}

func TestSSEResponseHeadersHTTPCommittedStatePreventsRetry(t *testing.T) {
	for _, tc := range []struct {
		name           string
		sameRetries    int
		channelRetries int
	}{
		{name: "same_channel", sameRetries: 2},
		{name: "channel_failover", channelRetries: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodyRelease := make(chan struct{})
			var releaseOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(bodyRelease) }) }
			defer resume()
			var attempts atomic.Int32
			fixture := newSSEResponseHeadersHTTPFixture(t, channel.TypeCodex, tc.sameRetries, tc.channelRetries, func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set(codex.TurnStateHeader, "committed-first-attempt")
				w.(http.Flusher).Flush()
				if attempt > 1 {
					_, _ = io.WriteString(w, sseResponseHeadersPayload())
					return
				}
				select {
				case <-bodyRelease:
					// EOF without model content triggers the normal pre-content retry policy.
				case <-r.Context().Done():
				}
			})
			response := fixture.post(t, "")
			defer response.Body.Close()
			require.Equal(t, "committed-first-attempt", response.Header.Get(codex.TurnStateHeader))
			assertSSEPreparationHeartbeats(t, response.Body, ": keep-alive\n\n")
			resume()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "event:error")
			require.Contains(t, string(body), `"type":"error"`)
			require.NotContains(t, string(body), "response.completed")
			fixture.wait(t)
			require.Equal(t, int32(1), attempts.Load(), "a committed state must never be paired with another attempt's content")
		})
	}
}

type sseResponseHeadersHTTPFixture struct {
	server *httptest.Server
	done   <-chan struct{}
}

func newSSEResponseHeadersHTTPFixture(t *testing.T, provider channel.Type, sameRetries, channelRetries int, handler http.HandlerFunc) *sseResponseHeadersHTTPFixture {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		handler(w, r)
	}))
	t.Cleanup(func() { upstream.CloseClientConnections(); upstream.Close() })
	client := enttest.NewEntClient(t, "sqlite3", "file:sse-response-headers-"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	project, err := client.Project.Create().SetName("sse-response-headers").Save(ctx)
	require.NoError(t, err)
	storage, err := client.DataStorage.Create().SetName("sse-response-headers").SetDescription("synthetic response-header storage").SetPrimary(true).SetType(datastorage.TypeDatabase).
		SetSettings(&objects.DataStorageSettings{}).SetStatus(datastorage.StatusActive).Save(ctx)
	require.NoError(t, err)
	channelService, requestService, systemService, usageService := setupSpeechAPITestServices(t, client)
	require.NoError(t, systemService.SetDefaultDataStorageID(ctx, storage.ID))
	require.NoError(t, systemService.SetRetryPolicy(ctx, &biz.RetryPolicy{
		Enabled: true, EmptyResponseDetection: true, LoadBalancerStrategy: biz.LoadBalancerStrategyAdaptive,
		MaxSingleChannelRetries: sameRetries, MaxChannelRetries: channelRetries, RetryDelayMs: 1,
	}))
	var candidates []*orchestrator.ChannelModelsCandidate
	var channels []*biz.Channel
	for index := 0; index <= channelRetries; index++ {
		name := "primary"
		if index > 0 {
			name = "backup"
		}
		row, err := client.Channel.Create().SetName(name).SetType(provider).SetBaseURL(upstream.URL).
			SetCredentials(objects.ChannelCredentials{APIKey: "synthetic-key"}).SetSupportedModels([]string{"gpt-test"}).SetDefaultTestModel("gpt-test").
			SetEndpoints([]objects.ChannelEndpoint{{APIFormat: llm.APIFormatOpenAIResponse.String(), Transport: objects.ChannelEndpointTransportHTTP}}).
			SetStatus(channel.StatusEnabled).Save(ctx)
		require.NoError(t, err)
		built, err := channelService.GetChannel(ctx, row.ID)
		require.NoError(t, err)
		channels = append(channels, built)
		candidates = append(candidates, &orchestrator.ChannelModelsCandidate{Channel: built, Models: []biz.ChannelModelEntry{{RequestModel: "gpt-test", ActualModel: "gpt-test", Source: "direct"}}})
	}
	channelService.SetEnabledChannelsForTest(channels)
	protection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{CacheConfig: xcache.Config{Mode: xcache.ModeMemory}, Ent: client})
	t.Cleanup(protection.Stop)
	handlers := NewOpenAIHandlers(OpenAIHandlersParams{
		ChannelService: channelService, RequestService: requestService, SystemService: systemService, UsageLogService: usageService,
		PromptService: biz.NewPromptService(biz.PromptServiceParams{Ent: client}), PromptProtectionRuleService: protection,
		HttpClient: httpclient.NewHttpClient(), LiveStreamRegistry: biz.NewLiveStreamRegistry(), ChannelLimiterManager: orchestrator.NewChannelLimiterManager(),
		SSEKeepAliveConfig: SSEKeepAliveConfig{Enabled: true, Interval: 500 * time.Millisecond},
	})
	handlers.ResponseCompletionHandlers.ChatCompletionOrchestrator = handlers.ResponseCompletionHandlers.ChatCompletionOrchestrator.WithChannelSelector(&streamDeliveryE2ESelector{candidates: candidates})
	engine := gin.New()
	done := make(chan struct{}, 4)
	engine.POST("/v1/responses", func(c *gin.Context) {
		defer func() { done <- struct{}{} }()
		requestCtx := authz.WithTestBypass(ent.NewContext(c.Request.Context(), client))
		c.Request = c.Request.WithContext(contexts.WithProjectID(requestCtx, project.ID))
		handlers.CreateResponse(c)
	})
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	server.Client().Timeout = 8 * time.Second
	return &sseResponseHeadersHTTPFixture{server: server, done: done}
}

func (f *sseResponseHeadersHTTPFixture) post(t *testing.T, state string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, f.server.URL+"/v1/responses", strings.NewReader(sseResponseHeadersRequest))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(codex.SessionHeader, "synthetic-codex-session")
	if state != "" {
		request.Header.Set(codex.TurnStateHeader, state)
	}
	response, err := f.server.Client().Do(request)
	require.NoError(t, err)
	return response
}

func (f *sseResponseHeadersHTTPFixture) wait(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("response-header handler did not finish")
	}
}

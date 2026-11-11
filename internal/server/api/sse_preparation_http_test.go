// 职责: 用真实 HTTP 连接验证各协议在上游响应头和首条内容到达前持续发送 SSE 心跳。
// 关联文件:
//
//	internal/server/api/chat.go
//	internal/server/api/gemini.go
//	internal/server/api/openai.go
//	internal/server/api/anthropic.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项:
//
//	上游必须等下游收到心跳后才继续，避免把收到首条内容之后的心跳误当成首包保护。
package api

import (
	"encoding/json"
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
	"github.com/looplj/axonhub/llm/httpclient"
)

type ssePreparationHTTPCase struct {
	name        string
	channelType channel.Type
	path        string
	body        string
	heartbeat   string
	payload     string
	terminal    string
	retries     int
}

func ssePreparationHTTPCases() []ssePreparationHTTPCase {
	return []ssePreparationHTTPCase{
		{
			name: "gemini", channelType: channel.TypeGemini,
			path:      "/gemini/v1beta/models/gpt-test:streamGenerateContent?alt=sse",
			body:      `{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`,
			heartbeat: "\n\n",
			payload: "data: {\"responseId\":\"gemini-stream\",\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]}}]}\n\n" +
				"data: {\"responseId\":\"gemini-stream\",\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"world\"}]},\"finishReason\":\"STOP\"}]}\n\n",
			terminal: `"finishReason":"STOP"`,
		},
		{
			name: "openai_chat", channelType: channel.TypeOpenai,
			path:      "/v1/chat/completions",
			body:      `{"model":"gpt-test","messages":[{"role":"user","content":"hello"}],"stream":true}`,
			heartbeat: ": keep-alive\n\n", payload: streamDeliveryChat(12, 34, true), terminal: "data: [DONE]",
		},
		{
			name: "anthropic", channelType: channel.TypeAnthropic,
			path:      "/v1/messages",
			body:      `{"model":"gpt-test","max_tokens":128,"messages":[{"role":"user","content":"hello"}],"stream":true}`,
			heartbeat: "event: ping\ndata: {\"type\":\"ping\"}\n\n",
			payload: strings.Replace(streamDeliveryAnthropic(12, 34), "event: message_delta", "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n"+
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n"+
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta", 1),
			terminal: "event:message_stop",
		},
		{
			name: "openai_responses", channelType: channel.TypeOpenaiResponses,
			path:      "/v1/responses",
			body:      `{"model":"gpt-test","input":"hello","stream":true}`,
			heartbeat: ": keep-alive\n\n",
			payload:   strings.Replace(streamDeliveryResponses(12, 34, "completed"), "event: response.completed", "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\nevent: response.completed", 1),
			terminal:  "event:response.completed",
		},
	}
}

func TestSSEPreparationHTTPHeartbeatBeforeFirstContent(t *testing.T) {
	for _, tc := range ssePreparationHTTPCases() {
		for _, stage := range []string{"response_headers", "first_event"} {
			t.Run(tc.name+"/"+stage, func(t *testing.T) {
				release := make(chan struct{})
				var releaseOnce sync.Once
				resume := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(resume)
				started := make(chan struct{})
				fixture := newSSEPreparationHTTPFixture(t, tc, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					if stage == "first_event" {
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
					}
					close(started)
					select {
					case <-release:
						_, _ = io.WriteString(w, tc.payload)
					case <-r.Context().Done():
					}
				})
				response := fixture.post(t, tc)
				defer response.Body.Close()
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.Contains(t, response.Header.Get("Content-Type"), "text/event-stream")
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("upstream did not reach the blocked stage")
				}

				// No upstream body exists until two complete heartbeat frames arrive.
				assertSSEPreparationHeartbeats(t, response.Body, tc.heartbeat)
				resume()
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				wire := string(body)
				require.Contains(t, wire, tc.terminal)
				require.NotContains(t, wire, "event:error")
				if tc.name == "gemini" {
					require.Contains(t, wire, `"text":"hello"`)
					require.Contains(t, wire, `"text":"world"`)
					require.Less(t, strings.Index(wire, `"text":"hello"`), strings.Index(wire, `"text":"world"`))
				}
				fixture.wait(t)
			})
		}
	}
}

func TestSSEPreparationHTTPHeartbeatAcrossRetry(t *testing.T) {
	for _, tc := range ssePreparationHTTPCases() {
		t.Run(tc.name, func(t *testing.T) {
			tc.retries = 1
			firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
			secondStarted := make(chan struct{})
			var firstOnce, secondOnce sync.Once
			resumeFirst := func() { firstOnce.Do(func() { close(firstRelease) }) }
			resumeSecond := func() { secondOnce.Do(func() { close(secondRelease) }) }
			t.Cleanup(func() { resumeFirst(); resumeSecond() })
			var attempts atomic.Int32
			fixture := newSSEPreparationHTTPFixture(t, tc, func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)
				release := firstRelease
				if attempt == 2 {
					close(secondStarted)
					release = secondRelease
				}
				select {
				case <-release:
					if attempt == 1 {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusServiceUnavailable)
						_, _ = io.WriteString(w, `{"error":{"message":"synthetic retryable failure"}}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, tc.payload)
				case <-r.Context().Done():
				}
			})
			response := fixture.post(t, tc)
			defer response.Body.Close()
			assertSSEPreparationHeartbeats(t, response.Body, tc.heartbeat)
			resumeFirst()
			select {
			case <-secondStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("the retry did not start")
			}
			assertSSEPreparationHeartbeats(t, response.Body, tc.heartbeat)
			resumeSecond()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, 1, strings.Count(string(body), tc.terminal))
			require.NotContains(t, string(body), "synthetic retryable failure")
			fixture.wait(t)
			require.Equal(t, int32(2), attempts.Load())
		})
	}
}

func TestSSEPreparationHTTPCancelBeforeFirstContent(t *testing.T) {
	for _, tc := range ssePreparationHTTPCases() {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			canceled := make(chan struct{})
			fixture := newSSEPreparationHTTPFixture(t, tc, func(_ http.ResponseWriter, r *http.Request) {
				close(started)
				<-r.Context().Done()
				close(canceled)
			})
			response := fixture.post(t, tc)
			defer response.Body.Close()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("upstream did not start before client cancellation")
			}
			assertSSEPreparationHeartbeats(t, response.Body, tc.heartbeat)
			require.NoError(t, response.Body.Close())
			select {
			case <-canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("client disconnect did not cancel the pending upstream request")
			}
			fixture.wait(t)
		})
	}
}

func TestSSEPreparationHTTPErrorAfterHeartbeat(t *testing.T) {
	for _, tc := range ssePreparationHTTPCases() {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(resume)
			fixture := newSSEPreparationHTTPFixture(t, tc, func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					body := `{"error":{"message":"synthetic upstream rejection","type":"invalid_request_error","code":"invalid_argument"}}`
					if tc.name == "gemini" {
						body = `{"error":{"message":"synthetic upstream rejection","code":400,"status":"INVALID_ARGUMENT"}}`
					}
					if tc.name == "anthropic" {
						body = `{"type":"error","error":{"message":"synthetic upstream rejection","type":"invalid_request_error"}}`
					}
					_, _ = io.WriteString(w, body)
				case <-r.Context().Done():
				}
			})
			response := fixture.post(t, tc)
			defer response.Body.Close()
			assertSSEPreparationHeartbeats(t, response.Body, tc.heartbeat)
			resume()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Contains(t, response.Header.Get("Content-Type"), "text/event-stream")
			require.Contains(t, string(body), "synthetic upstream rejection")
			require.NotContains(t, string(body), tc.terminal)
			var errorPayload map[string]any
			errorEvents := 0
			for _, line := range strings.Split(string(body), "\n") {
				if strings.HasPrefix(line, "data:") && strings.Contains(line, "synthetic upstream rejection") {
					errorEvents++
					require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &errorPayload))
				}
			}
			require.Equal(t, 1, errorEvents, "the upstream rejection must be one complete SSE data event")
			switch tc.name {
			case "anthropic":
				require.Equal(t, "error", errorPayload["type"])
				errorDetail, ok := errorPayload["error"].(map[string]any)
				require.True(t, ok)
				require.NotEmpty(t, errorDetail["type"])
			case "gemini":
				errorDetail, ok := errorPayload["error"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, float64(http.StatusBadRequest), errorDetail["code"])
				require.Equal(t, "INVALID_ARGUMENT", errorDetail["status"])
			case "openai_responses":
				require.Equal(t, "error", errorPayload["type"])
				require.Equal(t, float64(0), errorPayload["sequence_number"])
				require.Equal(t, "synthetic upstream rejection", errorPayload["message"])
			case "openai_chat":
				errorDetail, ok := errorPayload["error"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, "synthetic upstream rejection", errorDetail["message"])
			}
			fixture.wait(t)
		})
	}
}

func assertSSEPreparationHeartbeats(t *testing.T, reader io.Reader, heartbeat string) {
	t.Helper()
	for range 2 {
		frame := make([]byte, len(heartbeat))
		_, err := io.ReadFull(reader, frame)
		require.NoError(t, err)
		require.Equal(t, heartbeat, string(frame))
	}
}

type ssePreparationHTTPFixture struct {
	server *httptest.Server
	done   <-chan struct{}
}

func newSSEPreparationHTTPFixture(t *testing.T, tc ssePreparationHTTPCase, upstreamHandler http.HandlerFunc) *ssePreparationHTTPFixture {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// net/http starts detecting a peer disconnect once the request body is consumed.
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		upstreamHandler(w, r)
	}))
	t.Cleanup(func() {
		upstream.CloseClientConnections()
		upstream.Close()
	})
	dbName := strings.ReplaceAll(t.Name(), "/", "_")
	client := enttest.NewEntClient(t, "sqlite3", "file:sse-preparation-"+dbName+"?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	project, err := client.Project.Create().SetName("sse-preparation").Save(ctx)
	require.NoError(t, err)
	storage, err := client.DataStorage.Create().SetName("sse-preparation-storage").SetDescription("SSE preparation test storage").SetPrimary(true).
		SetType(datastorage.TypeDatabase).SetSettings(&objects.DataStorageSettings{}).SetStatus(datastorage.StatusActive).Save(ctx)
	require.NoError(t, err)
	channelRow, err := client.Channel.Create().SetType(tc.channelType).SetName("sse-preparation").SetBaseURL(upstream.URL).
		SetCredentials(objects.ChannelCredentials{APIKey: "synthetic-key"}).SetSupportedModels([]string{"gpt-test"}).SetDefaultTestModel("gpt-test").
		SetStatus(channel.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	channelService, requestService, systemService, usageService := setupSpeechAPITestServices(t, client)
	require.NoError(t, systemService.SetDefaultDataStorageID(ctx, storage.ID))
	// Empty-response pre-reading is the Process() path that previously hid heartbeats.
	require.NoError(t, systemService.SetRetryPolicy(ctx, &biz.RetryPolicy{
		Enabled: true, EmptyResponseDetection: true, LoadBalancerStrategy: biz.LoadBalancerStrategyAdaptive,
		MaxSingleChannelRetries: tc.retries, RetryDelayMs: 15,
	}))
	selectedChannel, err := channelService.GetChannel(ctx, channelRow.ID)
	require.NoError(t, err)
	channelService.SetEnabledChannelsForTest([]*biz.Channel{selectedChannel})
	promptProtection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{CacheConfig: xcache.Config{Mode: xcache.ModeMemory}, Ent: client})
	t.Cleanup(promptProtection.Stop)
	promptService := biz.NewPromptService(biz.PromptServiceParams{Ent: client})
	keepAlive := SSEKeepAliveConfig{Enabled: true, Interval: 5 * time.Millisecond}
	var handler *ChatCompletionHandlers
	var serve gin.HandlerFunc
	switch tc.name {
	case "gemini":
		h := NewGeminiHandlers(GeminiHandlersParams{
			ChannelService: channelService, RequestService: requestService, SystemService: systemService,
			UsageLogService: usageService, PromptService: promptService, PromptProtectionRuleService: promptProtection,
			HttpClient: httpclient.NewHttpClient(), LiveStreamRegistry: biz.NewLiveStreamRegistry(),
			ChannelLimiterManager: orchestrator.NewChannelLimiterManager(), SSEKeepAliveConfig: keepAlive,
		})
		handler, serve = h.ChatCompletionHandlers, h.GenerateContent
	case "anthropic":
		h := NewAnthropicHandlers(AnthropicHandlersParams{
			ChannelService: channelService, RequestService: requestService, SystemService: systemService,
			UsageLogService: usageService, PromptService: promptService, PromptProtectionRuleService: promptProtection,
			HttpClient: httpclient.NewHttpClient(), LiveStreamRegistry: biz.NewLiveStreamRegistry(),
			ChannelLimiterManager: orchestrator.NewChannelLimiterManager(), SSEKeepAliveConfig: keepAlive,
		})
		handler, serve = h.ChatCompletionHandlers, h.CreateMessage
	default:
		h := NewOpenAIHandlers(OpenAIHandlersParams{
			ChannelService: channelService, RequestService: requestService, SystemService: systemService,
			UsageLogService: usageService, PromptService: promptService, PromptProtectionRuleService: promptProtection,
			HttpClient: httpclient.NewHttpClient(), LiveStreamRegistry: biz.NewLiveStreamRegistry(),
			ChannelLimiterManager: orchestrator.NewChannelLimiterManager(), SSEKeepAliveConfig: keepAlive,
		})
		handler, serve = h.ChatCompletionHandlers, h.ChatCompletion
		if tc.name == "openai_responses" {
			handler, serve = h.ResponseCompletionHandlers, h.CreateResponse
		}
	}
	selector := &streamDeliveryE2ESelector{candidates: []*orchestrator.ChannelModelsCandidate{{
		Channel: selectedChannel, Models: []biz.ChannelModelEntry{{RequestModel: "gpt-test", ActualModel: "gpt-test", Source: "direct"}},
	}}}
	handler.ChatCompletionOrchestrator = handler.ChatCompletionOrchestrator.WithChannelSelector(selector)
	engine := gin.New()
	done := make(chan struct{})
	route := strings.Split(tc.path, "?")[0]
	if tc.name == "gemini" {
		route = "/gemini/v1beta/models/*action"
	}
	engine.POST(route, func(c *gin.Context) {
		defer close(done)
		ctx := authz.WithTestBypass(ent.NewContext(c.Request.Context(), client))
		c.Request = c.Request.WithContext(contexts.WithProjectID(ctx, project.ID))
		serve(c)
	})
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	server.Client().Timeout = 5 * time.Second
	return &ssePreparationHTTPFixture{server: server, done: done}
}

func (f *ssePreparationHTTPFixture) post(t *testing.T, tc ssePreparationHTTPCase) *http.Response {
	t.Helper()
	response, err := f.server.Client().Post(f.server.URL+tc.path, "application/json", strings.NewReader(tc.body))
	require.NoError(t, err)
	return response
}

func (f *ssePreparationHTTPFixture) wait(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE handler did not finish and release its preparation worker")
	}
}

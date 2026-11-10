package api

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestGeminiSSEHeartbeat(t *testing.T) {
	const model = "gemini-test"
	const firstEvent = `data: {"responseId":"gemini-stream","candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]}}]}` + "\n\n"
	const lastEvent = `data: {"responseId":"gemini-stream","candidates":[{"content":{"role":"model","parts":[{"text":"world"}]},"finishReason":"STOP"}]}` + "\n\n"
	resume := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, firstEvent)
		w.(http.Flusher).Flush()
		select {
		case <-resume:
			_, _ = io.WriteString(w, lastEvent)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)

	client := enttest.NewEntClient(t, "sqlite3", "file:gemini-heartbeat?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	project, err := client.Project.Create().SetName("gemini-heartbeat").Save(ctx)
	require.NoError(t, err)
	_, err = client.DataStorage.Create().SetName("gemini-heartbeat-storage").SetDescription("Gemini heartbeat test storage").SetPrimary(true).
		SetType(datastorage.TypeDatabase).SetSettings(&objects.DataStorageSettings{}).SetStatus(datastorage.StatusActive).Save(ctx)
	require.NoError(t, err)
	channelRow, err := client.Channel.Create().SetType(channel.TypeGemini).SetName("gemini-heartbeat").SetBaseURL(upstream.URL).
		SetCredentials(objects.ChannelCredentials{APIKey: "test-key"}).SetSupportedModels([]string{model}).SetDefaultTestModel(model).
		SetStatus(channel.StatusEnabled).Save(ctx)
	require.NoError(t, err)

	channelService, requestService, systemService, usageService := setupSpeechAPITestServices(t, client)
	selectedChannel, err := channelService.GetChannel(ctx, channelRow.ID)
	require.NoError(t, err)
	channelService.SetEnabledChannelsForTest([]*biz.Channel{selectedChannel})
	promptProtection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{CacheConfig: xcache.Config{Mode: xcache.ModeMemory}, Ent: client})
	t.Cleanup(promptProtection.Stop)
	handlers := NewGeminiHandlers(GeminiHandlersParams{
		ChannelService:              channelService,
		RequestService:              requestService,
		SystemService:               systemService,
		UsageLogService:             usageService,
		PromptService:               biz.NewPromptService(biz.PromptServiceParams{Ent: client}),
		PromptProtectionRuleService: promptProtection,
		HttpClient:                  httpclient.NewHttpClient(),
		LiveStreamRegistry:          biz.NewLiveStreamRegistry(),
		ChannelLimiterManager:       orchestrator.NewChannelLimiterManager(),
		SSEKeepAliveConfig:          SSEKeepAliveConfig{Enabled: true, Interval: 5 * time.Millisecond},
	})
	selector := &streamDeliveryE2ESelector{candidates: []*orchestrator.ChannelModelsCandidate{{
		Channel: selectedChannel,
		Models:  []biz.ChannelModelEntry{{RequestModel: model, ActualModel: model, Source: "direct"}},
	}}}
	handlers.ChatCompletionHandlers.ChatCompletionOrchestrator = handlers.ChatCompletionHandlers.ChatCompletionOrchestrator.WithChannelSelector(selector)

	engine := gin.New()
	engine.POST("/gemini/v1beta/models/*action", func(c *gin.Context) {
		ctx := authz.WithTestBypass(ent.NewContext(c.Request.Context(), client))
		c.Request = c.Request.WithContext(contexts.WithProjectID(ctx, project.ID))
		handlers.GenerateContent(c)
	})
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	server.Client().Timeout = 5 * time.Second
	response, err := server.Client().Post(server.URL+"/gemini/v1beta/models/"+model+":streamGenerateContent?alt=sse",
		"application/json", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Contains(t, response.Header.Get("Content-Type"), "text/event-stream")

	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		if line == "\n" {
			continue
		}
		require.True(t, strings.HasPrefix(line, "data: "), "%q", line)
		require.Contains(t, line, `"text":"hello"`)
		separator, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "\n", separator)
		break
	}

	// The upstream cannot produce its final event until the heartbeat has been received.
	heartbeat := make([]byte, 2)
	_, err = io.ReadFull(reader, heartbeat)
	require.NoError(t, err)
	require.Equal(t, "\n\n", string(heartbeat))
	close(resume)

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(body), `"text":"world"`)
	require.Contains(t, string(body), `"finishReason":"STOP"`)
	for _, frame := range strings.Split(string(body), "\n\n") {
		if frame != "" {
			require.True(t, strings.HasPrefix(frame, "data: "), "%q", frame)
		}
	}
}

// 职责: 用本地 Gemini 上游验证工具调用跨渠道接续，包括流式、透传和重试次数限制。
// 关联文件:
//   internal/server/orchestrator/gemini_signature.go
//   internal/server/orchestrator/outbound.go
//   internal/server/orchestrator/pass_through.go
//   llm/transformer/gemini/outbound.go
//   llm/transformer/gemini/openai/outbound.go

package orchestrator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer/gemini"
	geminiopenai "github.com/looplj/axonhub/llm/transformer/gemini/openai"
)

func TestGeminiSignatureFailover(t *testing.T) {
	const model = "gemini-4-neon"
	const original = "AQIDBAUG"
	const migrated = "skip_thought_signature_validator"
	const toolResponse = `{"candidates":[{"content":{"role":"model","parts":[
		{"functionCall":{"name":"read","args":{"n":1}},"thoughtSignature":"AQIDBAUG"},
		{"functionCall":{"name":"read","args":{"n":2}}}]},"finishReason":"STOP"}]}`
	const continuedResponse = `{"candidates":[{"content":{"role":"model","parts":[{"text":"continued"}]},"finishReason":"STOP"}]}`
	const firstUser = `{"role":"user","parts":[{"text":"read both"}]}`
	const results = `{"role":"user","parts":[{"functionResponse":{"name":"read","response":{"value":"one"}}},{"functionResponse":{"name":"read","response":{"value":"two"}}}]}`
	const rejection = "Invalid thought signature."
	studio, vertex := channel.TypeGemini, channel.TypeGeminiVertex
	type attempt struct {
		route, signature string
		status           int
		message          string
	}
	offline := attempt{"route0", original, 503, "resource exhausted"}
	refused := attempt{"route1", original, 400, rejection}
	fixed := attempt{"route1", migrated, 200, ""}
	tests := []struct {
		name        string
		routes      []channel.Type
		retries     int
		passThrough bool
		stream      bool
		alias       bool
		attempts    []attempt
	}{
		{"studio to vertex SSE pass-through", []channel.Type{studio, vertex}, 1, true, true, true, []attempt{offline, refused, fixed}},
		{"studio first-event timeout to vertex", []channel.Type{studio, vertex}, 1, true, true, false, []attempt{{"route0", original, 0, ""}, refused, fixed}},
		{"vertex to studio converted", []channel.Type{vertex, studio}, 1, false, false, true, []attempt{offline, refused, fixed}},
		{"vertex to Gemini OpenAI endpoint", []channel.Type{vertex, channel.TypeGeminiOpenai}, 1, false, false, false, []attempt{offline, refused, fixed}},
		{"target accepts original", []channel.Type{studio, vertex}, 1, true, false, false, []attempt{offline, {"route1", original, 200, ""}}},
		{"vertex to vertex", []channel.Type{vertex, vertex}, 0, false, false, false, []attempt{offline, {"route1", original, 200, ""}}},
		{"one attempt on each vertex backup", []channel.Type{studio, vertex, vertex}, 0, true, false, false, []attempt{offline, {"route1", migrated, 503, "busy"}, {"route2", migrated, 200, ""}}},
		{"different first route on next turn", []channel.Type{vertex}, 1, false, true, false, []attempt{{"route0", original, 400, "Thought signature is not valid."}, {"route0", migrated, 200, ""}}},
		{"second rejection restores original for next channel", []channel.Type{studio, vertex, studio}, 2, true, false, false, []attempt{offline, refused, {"route1", migrated, 400, rejection}, {"route2", original, 200, ""}}},
		{"unrelated bad request", []channel.Type{vertex}, 1, true, false, false, []attempt{{"route0", original, 400, "Invalid function argument."}}},
		{"disabled same-channel retry", []channel.Type{vertex}, 0, true, false, false, []attempt{{"route0", original, 400, rejection}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			type observed struct {
				path string
				body []byte
			}
			seen := make(chan observed, (tc.retries+1)*len(tc.routes)+1)
			steps := make(chan attempt, len(tc.attempts))
			for _, step := range tc.attempts {
				steps <- step
			}
			close(steps)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if gjson.GetBytes(body, "contents.#").Int() == 1 {
					fmt.Fprint(w, toolResponse)
					return
				}
				seen <- observed{path: r.URL.Path, body: body}
				step, ok := <-steps
				if !ok {
					http.Error(w, "unexpected extra attempt", http.StatusBadRequest)
					return
				}
				if step.status == 0 {
					w.Header().Set("Content-Type", "text/event-stream")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				if step.status != http.StatusOK {
					w.WriteHeader(step.status)
					fmt.Fprintf(w, `{"error":{"code":%d,"message":%q}}`, step.status, step.message)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/chat/completions") {
					fmt.Fprint(w, `{"id":"ok","model":"gemini-4-neon","choices":[{"index":0,"message":{"role":"assistant","content":"continued"},"finish_reason":"stop"}]}`)
					return
				}
				if strings.HasSuffix(r.URL.Path, ":streamGenerateContent") {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "data: %s\n\n", continuedResponse)
					return
				}
				fmt.Fprint(w, continuedResponse)
			}))
			defer upstream.Close()

			run := func(ctx context.Context, body, action string, routes []channel.Type) (*pipeline.Result, error) {
				state := &PersistenceState{OriginalModel: model, Settings: &RequestSettings{
					PassThrough: tc.passThrough, RetryPolicy: biz.RetryPolicy{Enabled: true, MaxSingleChannelRetries: tc.retries},
				}}
				for i, kind := range routes {
					name := fmt.Sprintf("route%d", i)
					config := gemini.Config{BaseURL: upstream.URL + "/" + name}
					if kind == vertex {
						config.PlatformType = gemini.PlatformVertex
					}
					out, err := gemini.NewOutboundTransformerWithConfig(config)
					if kind == channel.TypeGeminiOpenai {
						out, err = geminiopenai.NewOutboundTransformer(config.BaseURL, "test-key")
					}
					require.NoError(t, err)
					settings := &objects.ChannelSettings{}
					if i == 0 && len(routes) > 1 {
						settings.MaxSingleChannelRetries = lo.ToPtr(0)
					}
					models := []biz.ChannelModelEntry{{ActualModel: model, RequestModel: model}}
					if i == 1 && tc.alias {
						models = append(models, biz.ChannelModelEntry{ActualModel: "another-model", RequestModel: model})
					}
					state.ChannelModelsCandidates = append(state.ChannelModelsCandidates, &ChannelModelsCandidate{
						Channel: &biz.Channel{Channel: &ent.Channel{ID: i + 1, Type: kind, Name: name, Settings: settings}, Outbound: out},
						Models:  models, APIFormat: out.APIFormat().String(),
					})
				}
				in, out := NewPersistentTransformers(state, gemini.NewInboundTransformer())
				var firstEventTimeout time.Duration
				if tc.attempts[0].status == 0 {
					firstEventTimeout = time.Second
				}
				pipe := pipeline.NewFactory(httpclient.NewHttpClientWithClient(upstream.Client())).Pipeline(in, out,
					pipeline.WithRetry(len(routes)-1, tc.retries, 0),
					pipeline.WithResponseTimeouts(firstEventTimeout, 0),
					pipeline.WithMiddlewares(applyPassThroughResponse(out), applyPassThroughStream(out),
						applyPassThroughRequestBody(out, nil), finalizeTransportRequest(out), applyGeminiHistorySignatures(out),
						captureRawProviderResponse(out), captureRawProviderStream(out)),
				)
				request := &httpclient.Request{Path: "/v1beta/models/" + model + ":" + action, Body: []byte(body)}
				result, err := pipe.Process(ctx, request)
				require.Equal(t, body, string(request.Body), "client history")
				return result, err
			}

			// Send the model's actual first response back with both parallel tool results.
			initial := tc.routes[:1]
			if len(tc.routes) == 1 {
				initial = []channel.Type{studio}
			}
			first, err := run(t.Context(), `{"contents":[`+firstUser+`]}`, "generateContent", initial)
			require.NoError(t, err)
			calls := gjson.GetBytes(first.Response.Body, "candidates.0.content").Raw
			history := `{"contents":[` + firstUser + "," + calls + "," + results + `],"extraField":"keep"}`
			action := "generateContent"
			if tc.stream {
				action = "streamGenerateContent"
			}
			result, err := run(t.Context(), history, action, tc.routes)
			if last := tc.attempts[len(tc.attempts)-1]; last.status != http.StatusOK {
				require.ErrorContains(t, err, last.message)
			} else {
				require.NoError(t, err)
				if tc.stream {
					events, err := streams.All(result.EventStream)
					require.NoError(t, err)
					require.NoError(t, result.EventStream.Close())
					require.NotEmpty(t, events)
					require.Contains(t, string(events[0].Data), "continued")
				} else {
					require.Equal(t, "continued", gjson.GetBytes(result.Response.Body, "candidates.0.content.parts.0.text").Str)
				}
			}
			require.Len(t, seen, len(tc.attempts))
			for _, step := range tc.attempts {
				got := <-seen
				require.True(t, strings.HasPrefix(got.path, "/"+step.route+"/"), got.path)
				if strings.HasSuffix(got.path, "/chat/completions") {
					require.Equal(t, model, gjson.GetBytes(got.body, "model").Str)
					calls := gjson.GetBytes(got.body, "messages.1.tool_calls").Array()
					require.Len(t, calls, 2)
					require.Equal(t, step.signature, calls[0].Get("extra_content.google.thought_signature").Str)
					require.False(t, calls[1].Get("extra_content.google.thought_signature").Exists())
					for i, value := range []string{"one", "two"} {
						tool := gjson.GetBytes(got.body, fmt.Sprintf("messages.%d", i+2))
						require.Equal(t, calls[i].Get("id").Str, tool.Get("tool_call_id").Str)
						require.JSONEq(t, fmt.Sprintf(`{"value":%q}`, value), tool.Get("content").Str)
					}
					continue
				}
				require.True(t, strings.HasSuffix(got.path, "/models/"+model+":"+action), got.path)
				require.Equal(t, step.signature, gjson.GetBytes(got.body, "contents.1.parts.0.thoughtSignature").Str)
				require.False(t, gjson.GetBytes(got.body, "contents.1.parts.1.thoughtSignature").Exists())
				require.Equal(t, "1", gjson.GetBytes(got.body, "contents.1.parts.0.functionCall.args.n").Raw)
				require.Equal(t, "2", gjson.GetBytes(got.body, "contents.1.parts.1.functionCall.args.n").Raw)
				parts := gjson.GetBytes(got.body, "contents.2.parts").Array()
				require.Len(t, parts, 2)
				for i, value := range []string{"one", "two"} {
					response := parts[i].Get("functionResponse")
					require.Equal(t, "read", response.Get("name").Str)
					require.Equal(t, value, response.Get("response.value").Str)
					if !tc.passThrough {
						callID := gjson.GetBytes(got.body, fmt.Sprintf("contents.1.parts.%d.functionCall.id", i))
						require.Equal(t, callID.Str, response.Get("id").Str)
					}
				}
				if tc.passThrough {
					require.Equal(t, "keep", gjson.GetBytes(got.body, "extraField").Str)
				}
			}
		})
	}
}

func TestGeminiSignatureRetryRequiresGoogleEndpointAndUpstreamValidation(t *testing.T) {
	out := &PersistentOutboundTransformer{state: &PersistenceState{CurrentCandidate: &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{Type: channel.TypeOpenai}},
	}}}
	require.False(t, out.isGeminiHistoryRequest(&httpclient.Request{APIFormat: llm.APIFormatOpenAIChatCompletion.String()}))
	require.False(t, out.isGeminiHistoryRequest(&httpclient.Request{APIFormat: llm.APIFormatGeminiEmbedding.String()}))
	response := &llm.ResponseError{StatusCode: http.StatusBadRequest, Detail: llm.ErrorDetail{Message: "Invalid thought signature."}}
	require.False(t, isGeminiSignatureRejection(response))
	require.True(t, isGeminiSignatureRejection(pipeline.WrapUpstreamError(response)))
	response.StatusCode = http.StatusForbidden
	require.False(t, isGeminiSignatureRejection(pipeline.WrapUpstreamError(response)))
}

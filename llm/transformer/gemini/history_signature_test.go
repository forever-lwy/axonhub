// 职责: 验证 Gemini 工具历史迁移保留并行调用、工具结果、数值精度和透传字段。
// 关联文件:
//   llm/transformer/gemini/history_signature.go
//   llm/transformer/gemini/thinking_signature.go

package gemini

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/llm"
)

func TestMigrateHistorySignatures(t *testing.T) {
	const original = `{
		"extension":{"x":123},
		"contents":[
			{"role":"user","parts":[{"text":"start","thoughtSignature":"user-field"}]},
			{"role":"model","parts":[{"text":"","thought":true,"thoughtSignature":"tail"}]},
			{"role":"model","newField":123,"parts":[
				{"text":"thinking","thought":true,"thoughtSignature":"text-signature"},
				{"function_call":{"name":"read","args":{"n":9007199254740993,"thoughtSignature":"argument"}},"thought_signature":"foreign"},
				{"functionCall":{"name":"read","args":{"n":2}},"thoughtSignature":"second"},
				{"fileData":{"fileUri":"file"},"thoughtSignature":"media"}
			]},
			{"role":"user","parts":[{"functionResponse":{"name":"read","response":{"thoughtSignature":"output"}}}]},
			{"role":"model","parts":[{"functionCall":{"name":"next","id":"call-3"}},{"text":"","thoughtSignature":"trailer"}]}
		]
	}`
	input := []byte(original)
	body, changed := MigrateHistorySignatures(input, llm.APIFormatGeminiContents)
	require.True(t, changed)
	require.Equal(t, original, string(input))
	require.JSONEq(t, `{
		"extension":{"x":123},
		"contents":[
			{"role":"user","parts":[{"text":"start","thoughtSignature":"user-field"}]},
			{"role":"model","newField":123,"parts":[
				{"text":"thinking","thought":true},
				{"function_call":{"name":"read","args":{"n":9007199254740993,"thoughtSignature":"argument"}},"thoughtSignature":"skip_thought_signature_validator"},
				{"functionCall":{"name":"read","args":{"n":2}}},
				{"fileData":{"fileUri":"file"}}
			]},
			{"role":"user","parts":[{"functionResponse":{"name":"read","response":{"thoughtSignature":"output"}}}]},
			{"role":"model","parts":[{"functionCall":{"name":"next","id":"call-3"},"thoughtSignature":"skip_thought_signature_validator"}]}
		]
	}`, string(body))
	require.Equal(t, "9007199254740993", gjson.GetBytes(body, "contents.1.parts.1.function_call.args.n").Raw)
	again, changed := MigrateHistorySignatures(body, llm.APIFormatGeminiContents)
	require.False(t, changed)
	require.Equal(t, body, again)
}

func TestMigrateHistorySignaturesLeavesUnrelatedBodiesAlone(t *testing.T) {
	for _, input := range []string{
		`{"messages":[{"role":"assistant","content":"answer"}]}`,
		`{"contents":[{"role":"user","parts":[{"text":"thoughtSignature"}]},{"role":"model","parts":[{"text":"answer"}]}]}`,
		`{"contents":[{"role":"model","parts":[{"functionCall":{"name":"read"},"thoughtSignature":"skip_thought_signature_validator"}]}]}`,
	} {
		body, changed := MigrateHistorySignatures([]byte(input), llm.APIFormatGeminiContents)
		require.False(t, changed)
		require.Equal(t, input, string(body))
	}
}

func TestNativeHistoryMatchesParallelResultsOnce(t *testing.T) {
	var input GenerateContentRequest
	require.NoError(t, json.Unmarshal([]byte(`{"contents":[
		{"role":"model","parts":[
			{"function_call":{"name":"read","id":"first"},"thought_signature":"AQIDBAUG"},
			{"function_call":{"name":"read","id":"second"}},
			{"function_call":{"name":"read","id":"third"}}
		]},
		{"role":"user","parts":[{"function_response":{"name":"read","response":{"n":2}}}]},
		{"role":"user","parts":[
			{"function_response":{"name":"read","id":"first","response":{"n":1}}},
			{"function_response":{"name":"read","response":{"n":3}}}
		]},
		{"role":"model","parts":[{"text":"done"}]},
		{"role":"user","parts":[{"function_response":{"name":"read","response":{"n":4}}}]}
	]}`), &input))
	req, err := convertGeminiToLLMRequest(&input)
	require.NoError(t, err)
	require.Len(t, req.Messages, 6)
	require.Equal(t, "AQIDBAUG", *req.Messages[0].ReasoningSignature)
	require.Equal(t, "second", *req.Messages[1].ToolCallID)
	require.Equal(t, "first", *req.Messages[2].ToolCallID)
	require.Equal(t, "third", *req.Messages[3].ToolCallID)
	require.Empty(t, *req.Messages[5].ToolCallID)
}

func TestMigrateGeminiOpenAIHistory(t *testing.T) {
	const input = `{"messages":[
		{"role":"assistant","tool_calls":[
			{"id":"1","function":{"name":"read","arguments":"{}"},"extra_fields":{"other":1,"extra_content":{"google":{"thought_signature":"old"}}}},
			{"id":"2","function":{"name":"read","arguments":"{}"},"extra_content":{"google":{"thought_signature":"parallel","other":2}}}
		]},
		{"role":"tool","tool_call_id":"2","content":"thought_signature"}
	]}`
	body, changed := MigrateHistorySignatures([]byte(input), llm.APIFormatOpenAIChatCompletion)
	require.True(t, changed)
	require.JSONEq(t, `{"messages":[
		{"role":"assistant","tool_calls":[
			{"id":"1","function":{"name":"read","arguments":"{}"},"extra_fields":{"other":1,"extra_content":{"google":{}}},"extra_content":{"google":{"thought_signature":"skip_thought_signature_validator"}}},
			{"id":"2","function":{"name":"read","arguments":"{}"},"extra_content":{"google":{"other":2}}}
		]},
		{"role":"tool","tool_call_id":"2","content":"thought_signature"}
	]}`, string(body))
	again, changed := MigrateHistorySignatures(body, llm.APIFormatOpenAIChatCompletion)
	require.False(t, changed)
	require.Equal(t, body, again)
}

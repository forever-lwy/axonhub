// 职责: 将 Gemini 请求中的模型历史转换为可在另一后端接续的签名格式。
// 关联文件:
//   llm/transformer/gemini/thinking_signature.go
//   llm/transformer/gemini/openai/outbound.go
//   internal/server/orchestrator/gemini_signature.go
// 注意事项:
//   每个模型步骤只在首个并行工具调用上放迁移占位值。工具参数和结果中同名的字段属于用户数据。

package gemini

import (
	"encoding/json"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/looplj/axonhub/llm"
)

// MigrateHistorySignatures edits a copy of the final Gemini request body,
// keeping pass-through fields not represented by the transformers.
// A false result means the original body should be used.
func MigrateHistorySignatures(body []byte, format llm.APIFormat) ([]byte, bool) {
	key, role, migrate := "contents", "model", migrateNativeContent
	switch format {
	case llm.APIFormatGeminiContents:
	case llm.APIFormatOpenAIChatCompletion:
		key, role, migrate = "messages", "assistant", migrateOpenAIMessage
	default:
		return body, false
	}
	contents := gjson.GetBytes(body, key)
	if !contents.IsArray() {
		return body, false
	}

	changed := false
	items := contents.Array()
	kept := make([]json.RawMessage, 0, len(items))
	for _, content := range items {
		raw := json.RawMessage(content.Raw)
		if content.Get("role").Str == role {
			next, edited, err := migrate(content)
			if err != nil {
				return body, false
			}
			if edited {
				changed = true
				if len(next) == 0 {
					continue
				}
				raw = next
			}
		}
		kept = append(kept, raw)
	}
	if !changed || len(kept) == 0 {
		return body, false
	}
	next, err := sjson.SetBytes(body, key, kept)
	if err != nil {
		return body, false
	}
	return next, true
}

func migrateNativeContent(content gjson.Result) (json.RawMessage, bool, error) {
	var parts []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(content.Get("parts").Raw), &parts); err != nil {
		return nil, false, err
	}
	placeholder, _ := json.Marshal(ContextEngineeringThoughtSignature)
	changed, seenCall := false, false
	kept := parts[:0]
	for _, part := range parts {
		camel, snake := part["thoughtSignature"], part["thought_signature"]
		if !seenCall && (gjson.ParseBytes(part["functionCall"]).IsObject() || gjson.ParseBytes(part["function_call"]).IsObject()) {
			seenCall = true
			if string(camel) != string(placeholder) || snake != nil {
				delete(part, "thought_signature")
				part["thoughtSignature"] = placeholder
				changed = true
			}
		} else if camel != nil || snake != nil {
			delete(part, "thoughtSignature")
			delete(part, "thought_signature")
			changed = true
			if emptySignaturePart(part) {
				continue
			}
		}
		kept = append(kept, part)
	}
	if !changed || len(kept) == 0 {
		return nil, changed, nil
	}
	raw, err := sjson.Set(content.Raw, "parts", kept)
	return json.RawMessage(raw), true, err
}

func emptySignaturePart(part map[string]json.RawMessage) bool {
	for key, value := range part {
		if key != "thought" && (key != "text" || string(value) != `""`) {
			return false
		}
	}
	return true
}

func migrateOpenAIMessage(message gjson.Result) (json.RawMessage, bool, error) {
	const path = "extra_content.google.thought_signature"
	const wrapped = "extra_fields." + path
	calls := message.Get("tool_calls").Array()
	kept := make([]json.RawMessage, 0, len(calls))
	changed, seenCall := false, false
	for _, call := range calls {
		raw := call.Raw
		var err error
		if call.Get("function").IsObject() {
			if !seenCall && call.Get(path).Str != ContextEngineeringThoughtSignature {
				raw, err = sjson.Set(raw, path, ContextEngineeringThoughtSignature)
			} else if seenCall && call.Get(path).Exists() {
				raw, err = sjson.Delete(raw, path)
			}
			if err != nil {
				return nil, false, err
			}
			if call.Get(wrapped).Exists() {
				raw, err = sjson.Delete(raw, wrapped)
				if err != nil {
					return nil, false, err
				}
			}
			seenCall = true
		}
		changed = changed || raw != call.Raw
		kept = append(kept, json.RawMessage(raw))
	}
	if !changed {
		return nil, false, nil
	}
	raw, err := sjson.Set(message.Raw, "tool_calls", kept)
	return json.RawMessage(raw), true, err
}

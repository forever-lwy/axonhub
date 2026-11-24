package gemini

import (
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/transformer/shared"
)

// ContextEngineeringThoughtSignature is Google's REST placeholder for imported
// function calls. The validator expects this literal string on the wire.
var ContextEngineeringThoughtSignature = "skip_thought_signature_validator"

const transformerMetadataKeyGoogleThoughtSignature = shared.TransformerMetadataKeyGoogleThoughtSignature

func setInboundToolCallThoughtSignature(toolCall *llm.ToolCall, signature string) {
	if toolCall == nil || signature == "" {
		return
	}

	if toolCall.TransformerMetadata == nil {
		toolCall.TransformerMetadata = map[string]any{}
	}

	toolCall.TransformerMetadata[transformerMetadataKeyGoogleThoughtSignature] = signature
}

func getInboundGeminiToolCallThoughtSignature(toolCall llm.ToolCall) *string {
	if toolCall.TransformerMetadata == nil {
		return nil
	}

	raw, ok := toolCall.TransformerMetadata[transformerMetadataKeyGoogleThoughtSignature].(string)
	if !ok || raw == "" {
		return nil
	}

	return &raw
}

func setOutboundToolCallThoughtSignature(toolCall *llm.ToolCall, signature string) {
	if toolCall == nil || signature == "" {
		return
	}

	if toolCall.TransformerMetadata == nil {
		toolCall.TransformerMetadata = map[string]any{}
	}

	encoded := shared.EncodeGeminiThoughtSignature(&signature)
	if encoded == nil {
		return
	}

	toolCall.TransformerMetadata[transformerMetadataKeyGoogleThoughtSignature] = *encoded
}

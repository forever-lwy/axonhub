package shared

import "github.com/looplj/axonhub/llm"

// TransformerMetadataKeyGoogleThoughtSignature is the key used to store Gemini thought signature
// in ToolCall TransformerMetadata.
const TransformerMetadataKeyGoogleThoughtSignature = "google_thought_signature"

// EncodeGeminiThoughtSignature encodes a raw Gemini thought signature for storage.
// Gemini thought signatures are already base64-encoded, so this is a passthrough.
func EncodeGeminiThoughtSignature(signature *string) *string {
	if signature == nil {
		return nil
	}
	return signature
}

// DecodeGeminiThoughtSignature checks whether a signature blob is safe to use as a Gemini thought signature.
// Returns the raw value only if the blob is recognized as a Gemini signature.
// Returns nil for signatures from other providers (OpenAI/Anthropic) or unknown formats.
func DecodeGeminiThoughtSignature(signature *string) *string {
	if signature == nil {
		return nil
	}

	result := GuessSignatureProvider(*signature)
	if result.Provider == ProviderGemini {
		return signature
	}

	return nil
}

// GeminiReplaySignature keeps native Gemini signatures opaque. Vertex can
// return signatures without the protobuf envelope used to identify providers
// when a generic reasoning field is replayed across protocols.
func GeminiReplaySignature(signature *string, source llm.APIFormat) *string {
	if signature == nil || *signature == "" {
		return nil
	}
	if source == llm.APIFormatGeminiContents {
		return signature
	}
	return DecodeGeminiThoughtSignature(signature)
}

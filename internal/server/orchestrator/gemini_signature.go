// 职责: 在 Gemini 渠道拒绝历史签名时，准备同模型的工具历史接续。
// 关联文件:
//   internal/server/orchestrator/outbound.go
//   internal/server/orchestrator/orchestrator.go
//   llm/transformer/gemini/history_signature.go
// 注意事项:
//   迁移只作用于当前目标的最终请求体。原始历史还要用于其他渠道及后续重试。

package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/transformer/gemini"
)

type geminiSignatureReplay uint8

const (
	keepGeminiHistory geminiSignatureReplay = iota
	retryGeminiHistory
	migrateGeminiHistory
)

func applyGeminiHistorySignatures(p *PersistentOutboundTransformer) pipeline.Middleware {
	return pipeline.OnRawRequest("gemini_history_signatures", func(_ context.Context, req *httpclient.Request) (*httpclient.Request, error) {
		if !p.isGeminiHistoryRequest(req) {
			return req, nil
		}
		p.state.RawProviderRequest = req
		if p.geminiReplay == migrateGeminiHistory || p.geminiFailoverWithoutSameChannelRetry() {
			req.Body, _ = gemini.MigrateHistorySignatures(req.Body, llm.APIFormat(req.APIFormat))
			p.geminiReplay = migrateGeminiHistory
		}
		return req, nil
	})
}

// With no same-channel retry budget the new backend gets only one request.
// Migrate on that attempt only for a known Studio/Vertex route change on the
// same model. Comparing to the original route also covers a second Vertex backup.
func (p *PersistentOutboundTransformer) geminiFailoverWithoutSameChannelRetry() bool {
	if p.state.CurrentCandidateIndex == 0 || len(p.state.ChannelModelsCandidates) == 0 {
		return false
	}
	first := p.state.ChannelModelsCandidates[0]
	current := p.GetCurrentChannel()
	if first == nil || first.Channel == nil || first.Channel.Channel == nil || len(first.Models) == 0 ||
		current == nil || current.Channel == nil || first.Models[0].ActualModel != p.GetCurrentModelID() {
		return false
	}
	limit, overridden := p.SameChannelRetryLimit()
	if !overridden {
		limit = p.state.settings().RetryPolicy.MaxSingleChannelRetries
	}
	if limit > 0 {
		return false
	}
	from, to := first.Channel.Type, current.Type
	return (from == channel.TypeGeminiVertex && isGeminiAPIChannel(to)) ||
		(to == channel.TypeGeminiVertex && isGeminiAPIChannel(from))
}

func isGeminiAPIChannel(kind channel.Type) bool {
	return kind == channel.TypeGemini || kind == channel.TypeZenmuxGemini || kind == channel.TypeGeminiOpenai
}

func (p *PersistentOutboundTransformer) isGeminiHistoryRequest(req *httpclient.Request) bool {
	if req == nil {
		return false
	}
	switch llm.APIFormat(req.APIFormat) {
	case llm.APIFormatGeminiContents:
		return true
	case llm.APIFormatOpenAIChatCompletion:
		ch := p.GetCurrentChannel()
		return ch != nil && ch.Channel != nil && (isGeminiAPIChannel(ch.Type) || ch.Type == channel.TypeGeminiVertex)
	default:
		return false
	}
}

func isGeminiSignatureRejection(err error) bool {
	if !pipeline.IsUpstreamError(err) {
		return false
	}
	response, ok := errors.AsType[*llm.ResponseError](err)
	if !ok || response.StatusCode != http.StatusBadRequest {
		return false
	}
	message := strings.ToLower(response.Detail.Message)
	message = strings.ReplaceAll(message, "thought_signature", "thought signature")
	message = strings.ReplaceAll(message, "thoughtsignature", "thought signature")
	return strings.Contains(message, "invalid thought signature") ||
		strings.Contains(message, "corrupted thought signature") ||
		strings.Contains(message, "thought signature is not valid") ||
		strings.Contains(message, "thought signature is invalid") ||
		strings.Contains(message, "missing a thought signature")
}

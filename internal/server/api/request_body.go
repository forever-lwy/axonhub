// 职责: 读取模型 HTTP 请求，并把大小超限转换为客户端错误和项目失败记录。
// 关联文件:
//   internal/server/middleware/request_body.go
//   internal/server/biz/request.go
//   internal/server/api/chat.go
//   llm/httpclient/utils.go
//   llm/transformer/gemini/inbound.go
// 注意事项:
//   拒绝记录只保存网关生成的诊断信息。请求体和携带凭证的查询参数不得写入。

package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/pkg/xjson"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/middleware"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/gemini"
)

func readModelRequest(c *gin.Context) (*httpclient.Request, error) {
	return httpclient.ReadHTTPRequestWithLimit(c.Request, middleware.LLMRequestBodyLimit(c))
}

func writeModelReadError(c *gin.Context, err error, orch *orchestrator.ChatCompletionOrchestrator) {
	if errors.Is(err, httpclient.ErrRequestBodyTooLarge) {
		err = recordOversizedRequest(c, err, orch.RequestService)
	}
	httpErr := orch.Inbound.TransformError(c.Request.Context(), err)
	c.JSON(httpErr.StatusCode, json.RawMessage(httpErr.Body))
}

func recordOversizedRequest(c *gin.Context, err error, requests *biz.RequestService) *llm.ResponseError {
	_ = c.Error(err)
	ctx := c.Request.Context()
	requestID, _ := contexts.GetRequestID(ctx)
	const code = "request_body_too_large"
	omission := gin.H{
		"omitted":     code,
		"http_status": http.StatusRequestEntityTooLarge,
		"message":     err.Error(),
		"limit_bytes": middleware.LLMRequestBodyLimit(c),
		"route":       c.FullPath(),
		"request_id":  requestID,
	}
	if c.Request.ContentLength >= 0 {
		omission["content_length"] = c.Request.ContentLength
	}
	// Gemini puts the model in the URL. Other HTTP APIs carry it inside the rejected body.
	var model string
	var stream bool
	var format llm.APIFormat
	if action := c.Param("action"); action != "" {
		model, stream, _ = gemini.ParseGenerateContentPath(action)
		format = llm.APIFormatGeminiContents
	}
	if saveErr := requests.RecordRejectedRequest(ctx, model, format, stream, xjson.MustMarshal(omission)); saveErr != nil {
		log.Warn(ctx, "Failed to record oversized request", log.Cause(saveErr))
	}
	return &llm.ResponseError{
		StatusCode: http.StatusRequestEntityTooLarge,
		Detail: llm.ErrorDetail{
			Type:      "invalid_request_error",
			Code:      code,
			Message:   err.Error(),
			RequestID: requestID,
		},
		Cause: err,
	}
}

// 职责: 为模型 HTTP 路由提供请求体上限，并限制追踪中间件预读的字节数。
// 关联文件:
//   internal/server/routes.go
//   internal/server/middleware/trace.go
//   internal/server/api/request_body.go
//   llm/httpclient/utils.go
// 注意事项:
//   预读发现超限时保留读取错误，由模型接口按客户端协议返回 413。

package middleware

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

const DefaultMaxRequestBodySize = 128 << 20

const llmRequestBodyLimitKey = "axonhub.llmRequestBodyLimit"

func WithLLMRequestBodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(llmRequestBodyLimitKey, maxBytes)
		c.Next()
	}
}

func LLMRequestBodyLimit(c *gin.Context) int64 {
	if limit := c.GetInt64(llmRequestBodyLimitKey); limit > 0 {
		return limit
	}
	return DefaultMaxRequestBodySize
}

func readTraceBody(c *gin.Context) ([]byte, error) {
	limit := LLMRequestBodyLimit(c)
	if c.Request.Body == nil || c.Request.ContentLength > limit {
		return nil, nil
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	body, err := io.ReadAll(c.Request.Body)
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		// MaxBytesReader repeats this error on subsequent reads, even for a chunked body.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

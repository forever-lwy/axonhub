// 职责: 验证主服务在根路径和配置的前缀下通过同一端口支持 HTTP/1.1 和 h2c，并能完整读取大请求体。
// 关联文件:
//   internal/server/server.go

package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestServerHTTPProtocols(t *testing.T) {
	t.Run("root", func(t *testing.T) { testServerHTTPProtocolsAt(t, "") })
	t.Run("prefix", func(t *testing.T) { testServerHTTPProtocolsAt(t, "/axonhub") })
}

func testServerHTTPProtocolsAt(t *testing.T, basePath string) {
	t.Helper()
	srv := New(Config{BasePath: basePath})
	srv.POST("/upload", func(c *gin.Context) {
		size, err := io.Copy(io.Discard, c.Request.Body)
		if err != nil {
			c.String(http.StatusBadRequest, "%v", err)
			return
		}
		c.String(http.StatusOK, "%d", size)
	})

	server := httptest.NewUnstartedServer(nil)
	server.Config = srv.newHTTPServer()
	server.Start()
	t.Cleanup(server.Close)

	for _, tc := range []struct {
		protocol string
		h2c      bool
		bodySize int
	}{
		{"HTTP/1.1", false, 16},
		{"HTTP/2.0", true, 33 << 20},
	} {
		t.Run(tc.protocol, func(t *testing.T) {
			var protocols http.Protocols
			protocols.SetHTTP1(!tc.h2c)
			protocols.SetUnencryptedHTTP2(tc.h2c)
			client := &http.Client{Transport: &http.Transport{Protocols: &protocols}, Timeout: 30 * time.Second}
			t.Cleanup(client.CloseIdleConnections)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+basePath+"/upload", bytes.NewReader(make([]byte, tc.bodySize)))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.protocol, resp.Proto)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, strconv.Itoa(tc.bodySize), string(body))
		})
	}
}

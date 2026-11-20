// 职责: 验证按需模式下的视频存储任务能在空闲时停查、新视频出现后继续保存。
// 关联文件:
//   internal/server/video_storage/worker.go
//   internal/server/biz/scheduled_check.go
//   internal/server/biz/data_storage.go

package video_storage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/datastorage"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
)

func TestOnDemandVideoStorageResumesForNewWork(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	cfg := xcache.Config{Mode: xcache.ModeMemory, OnDemand: true}
	system := biz.NewSystemService(biz.SystemServiceParams{Ent: client, CacheConfig: cfg})
	storage := biz.NewDataStorageService(biz.DataStorageServiceParams{Client: client, SystemService: system, CacheConfig: cfg})
	w := NewWorker(Params{Ent: client, SystemService: system, DataStorageService: storage, CacheConfig: cfg})
	var queries int
	client.Intercept(ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, q ent.Query) (ent.Value, error) {
			queries++
			return next.Query(ctx, q)
		})
	}))
	w.runScanWithSystemContext(ctx)
	require.Positive(t, queries)
	queries = 0
	w.runScanWithSystemContext(ctx)
	require.Zero(t, queries)

	dir := t.TempDir()
	ds := client.DataStorage.Create().SetName("video").SetDescription("test").SetType(datastorage.TypeFs).
		SetSettings(&objects.DataStorageSettings{Directory: &dir}).SaveX(ctx)
	require.NoError(t, system.SetVideoStorageSettings(ctx, biz.VideoStorageSettings{Enabled: true, DataStorageID: ds.ID}))
	w.runScanWithSystemContext(ctx)
	queries = 0
	w.runScanWithSystemContext(ctx)
	require.Zero(t, queries)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("video"))
	}))
	defer server.Close()
	response, err := json.Marshal(llm.VideoResponse{VideoURL: server.URL + "/file.mp4"})
	require.NoError(t, err)
	project := client.Project.Create().SetName("video").SaveX(ctx)
	video := client.Request.Create().SetProjectID(project.ID).SetModelID("fixture").
		SetFormat(string(llm.APIFormatOpenAIVideo)).SetStatus(request.StatusCompleted).
		SetRequestBody(objects.JSONRawMessage("{}")).SetResponseBody(response).SaveX(ctx)
	w.runScanWithSystemContext(ctx)
	saved := client.Request.GetX(ctx, video.ID)
	require.True(t, saved.ContentSaved)
	require.NotNil(t, saved.ContentStorageID)
	require.Equal(t, ds.ID, *saved.ContentStorageID)
	w.runScanWithSystemContext(ctx)
	queries = 0
	w.runScanWithSystemContext(ctx)
	require.Zero(t, queries)
}

// 职责: 验证按需模式的空闲任务、事务提交和渠道到期恢复。
// 关联文件:
//   internal/server/biz/scheduled_check.go
//   internal/server/biz/channel_apikey.go
//   internal/server/biz/channel_probe_internal.go
//   internal/server/biz/provider_quota_internal.go
// 注意事项: 统计真实的 Ent 查询，触发定时任务时保留生产使用的回调。

package biz

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/llm/httpclient"
)

func TestOnDemandIdleTasksAndChannelRecovery(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	t.Cleanup(func() { client.Close() })
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	cfg := xcache.Config{Mode: xcache.ModeMemory, OnDemand: true}
	system := NewSystemService(SystemServiceParams{Ent: client, CacheConfig: cfg})
	http := httpclient.NewHttpClient()
	channels := NewChannelService(ChannelServiceParams{Ent: client, CacheConfig: cfg, SystemService: system, HttpClient: http})
	t.Cleanup(channels.Stop)
	probes := NewChannelProbeService(ChannelProbeServiceParams{Ent: client, CacheConfig: cfg, SystemService: system})
	quota := NewProviderQuotaService(ProviderQuotaServiceParams{Ent: client, CacheConfig: cfg, SystemService: system, HttpClient: http})
	storage := NewDataStorageService(DataStorageServiceParams{Client: client, CacheConfig: cfg, SystemService: system})
	prompts := NewPromptService(PromptServiceParams{Ent: client, CacheConfig: cfg})
	require.NoError(t, prompts.Initialize(ctx))
	var queries atomic.Int64
	client.Intercept(ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, q ent.Query) (ent.Value, error) {
			queries.Add(1)
			return next.Query(ctx, q)
		})
	}))
	tick := func() {
		channels.runChannelRecovery(ctx)
		channels.runSyncChannelModelsPeriodically(ctx)
		probes.runProbePeriodically(ctx)
		quota.runQuotaCheckScheduled(ctx)
		storage.reloadFileSystemsPeriodically(ctx)
		prompts.loadPromptsPeriodic(ctx)
	}
	tick()
	require.Positive(t, queries.Swap(0))
	for range 10 {
		tick()
	}
	require.Zero(t, queries.Load())

	ch := createTestChannelWithAPIKeys(t, client, ctx, "temporary", []string{"fixture"})
	expired := time.Now().Add(-time.Minute)
	client.Channel.UpdateOneID(ch.ID).SetStatus(channel.StatusDisabled).
		SetAutoDisabledAt(expired.Add(-time.Hour)).SetAutoDisableExpiresAt(expired).ExecX(ctx)
	channels.runChannelRecovery(ctx)
	require.Equal(t, channel.StatusEnabled, client.Channel.GetX(ctx, ch.ID).Status)
	// A manual disable has no recovery deadline.
	client.Channel.UpdateOneID(ch.ID).SetStatus(channel.StatusDisabled).ExecX(ctx)
	channels.runChannelRecovery(ctx)
	require.Equal(t, channel.StatusDisabled, client.Channel.GetX(ctx, ch.ID).Status)
	queries.Store(0)
	channels.runChannelRecovery(ctx)
	require.Zero(t, queries.Load())
}

func TestScheduledCheckObservesCommittedWrites(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	ch := createTestChannelWithAPIKeys(t, client, ctx, "initial", []string{"fixture"})
	check := NewScheduledCheck(client.Channel.Use)
	var calls int
	work := func(bool) (time.Time, error) {
		calls++
		return time.Time{}, nil
	}
	require.NoError(t, check.Run(work))
	for _, commit := range []bool{false, true} {
		tx, err := client.Tx(ctx)
		require.NoError(t, err)
		tx.Channel.UpdateOneID(ch.ID).SetName("changed").ExecX(ctx)
		require.NoError(t, check.Run(work))
		require.Equal(t, 1, calls)
		if commit {
			require.NoError(t, tx.Commit())
		} else {
			require.NoError(t, tx.Rollback())
			require.NoError(t, check.Run(work))
			require.Equal(t, 1, calls)
		}
	}
	require.NoError(t, check.Run(func(changed bool) (time.Time, error) {
		require.True(t, changed)
		client.Channel.UpdateOneID(ch.ID).SetName("during-check").ExecX(ctx)
		return time.Time{}, nil
	}))
	require.NoError(t, check.Run(work))
	require.Equal(t, 2, calls)
}

func TestScheduledCheckWaitsForDeadlineAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := NewScheduledCheck()
		require.NoError(t, check.Run(func(changed bool) (time.Time, error) {
			require.True(t, changed)
			return time.Now().Add(time.Hour), nil
		}))
		work := func(bool) (time.Time, error) { return time.Time{}, errors.New("retry") }
		time.Sleep(59 * time.Minute)
		require.NoError(t, check.Run(work))
		time.Sleep(time.Minute)
		require.ErrorContains(t, check.Run(work), "retry")
		require.NoError(t, check.Run(func(changed bool) (time.Time, error) {
			require.False(t, changed)
			return time.Time{}, nil
		}))
		require.NoError(t, check.Run(work))
	})
}

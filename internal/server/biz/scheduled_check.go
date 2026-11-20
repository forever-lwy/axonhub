// 职责: 在单实例中按已提交的数据变更和下一次到期时间执行后台检查。
// 关联文件:
//   internal/server/biz/transaction_hook.go
//   internal/server/biz/channel_apikey.go
//   internal/server/biz/provider_quota_internal.go
//   internal/server/video_storage/worker.go
// 注意事项: 启动时要检查已有数据。执行期间发生的变更须留到下一轮处理。

package biz

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/looplj/axonhub/internal/ent"
)

// ScheduledCheck keeps a cron tick local until a relevant write commits or
// work is due. A nil check retains the usual polling behavior.
type ScheduledCheck struct {
	revision atomic.Uint64
	mu       sync.Mutex
	seen     uint64
	next     time.Time
}

func NewScheduledCheck(subscribe ...func(...ent.Hook)) *ScheduledCheck {
	check := &ScheduledCheck{}
	check.revision.Store(1) // Discover work persisted before startup.
	for _, register := range subscribe {
		register(func(next ent.Mutator) ent.Mutator {
			return ent.MutateFunc(func(ctx context.Context, mutation ent.Mutation) (ent.Value, error) {
				v, err := next.Mutate(ctx, mutation)
				if err == nil {
					runAfterMutationCommit(ctx, mutation, func(context.Context) { check.revision.Add(1) })
				}
				return v, err
			})
		})
	}
	return check
}

type scheduledSettingsKey struct{}

// SettingsContext reads committed settings directly when a check can go dormant.
// A write hook can run before the settings service finishes evicting its old value.
func (s *ScheduledCheck) SettingsContext(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, scheduledSettingsKey{}, true)
}

// Run retries errors on the next tick. A zero deadline waits for a new write.
func (s *ScheduledCheck) Run(work func(changed bool) (time.Time, error)) error {
	if s == nil {
		_, err := work(true)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	revision := s.revision.Load()
	changed := revision != s.seen
	if !changed && (s.next.IsZero() || time.Now().Before(s.next)) {
		return nil
	}
	next, err := work(changed)
	if err != nil {
		return err
	}
	s.seen, s.next = revision, next
	return nil
}

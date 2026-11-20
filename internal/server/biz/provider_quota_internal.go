package biz

import (
	"context"
	"time"

	"github.com/looplj/axonhub/internal/authz"
)

func (svc *ProviderQuotaService) runQuotaCheckScheduled(ctx context.Context) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	ctx = authz.WithSystemBypass(ctx, "provider_quota")
	_ = svc.scheduledCheck.Run(func(bool) (time.Time, error) {
		return svc.runQuotaCheck(ctx, false), nil
	})
}

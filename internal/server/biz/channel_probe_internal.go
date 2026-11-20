package biz

import (
	"context"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/log"
)

func (svc *ChannelProbeService) runProbePeriodically(ctx context.Context) {
	ctx = authz.WithSystemBypass(ctx, "channel_probe")
	err := svc.probeCheck.Run(func(changed bool) (time.Time, error) {
		setting, err := svc.SystemService.ChannelSetting(svc.probeCheck.SettingsContext(ctx))
		if err != nil {
			return time.Time{}, err
		}
		now := time.Now().UTC()
		if err := svc.runProbeForSettings(ctx, setting, now); err != nil {
			return time.Time{}, err
		}
		if changed && setting.Probe.Enabled {
			// The newest execution may belong to the still-open aggregation window.
			window := time.Duration(setting.Probe.GetIntervalMinutes()) * time.Minute
			return now.Truncate(window).Add(window), nil
		}
		return time.Time{}, nil
	})
	if err != nil {
		log.Warn(ctx, "failed to collect channel probe data", log.Cause(err))
	}
}

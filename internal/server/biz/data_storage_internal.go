package biz

import (
	"context"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/log"
)

func (s *DataStorageService) reloadFileSystemsPeriodically(ctx context.Context) {
	err := s.refreshCheck.Run(func(bool) (time.Time, error) {
		err := authz.RunWithSystemBypassVoid(ctx, "refresh data storage filesystems", s.refreshFileSystems)
		return time.Time{}, err
	})
	if err != nil {
		log.Error(ctx, "failed to refresh data storage filesystems", log.Cause(err))
	}
}

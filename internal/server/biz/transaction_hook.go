package biz

import (
	"context"

	"github.com/looplj/axonhub/internal/ent"
)

// runAfterMutationCommit also recognizes callers that use tx.Client() without
// putting the Ent transaction in the context.
func runAfterMutationCommit(ctx context.Context, mutation ent.Mutation, fn func(context.Context)) {
	if m, ok := mutation.(interface{ Tx() (*ent.Tx, error) }); ok {
		if tx, err := m.Tx(); err == nil {
			ctx = ent.NewTxContext(ctx, tx)
		}
	}
	runAfterCommit(ctx, fn)
}

// runAfterCommit publishes an external side effect only after the surrounding
// Ent transaction commits successfully. Calls without a transaction have
// already committed when their mutation returns, so they run immediately.
func runAfterCommit(ctx context.Context, fn func(context.Context)) {
	callbackCtx := context.WithoutCancel(ctx)
	if tx := ent.TxFromContext(ctx); tx != nil {
		tx.OnCommit(func(next ent.Committer) ent.Committer {
			return ent.CommitFunc(func(commitCtx context.Context, tx *ent.Tx) error {
				if err := next.Commit(commitCtx, tx); err != nil {
					return err
				}

				fn(callbackCtx)
				return nil
			})
		})
		return
	}

	fn(callbackCtx)
}

package gosqlrwdb

import (
	"context"
)

const (
	// ContextUsePrimaryKey is the context key for using primary DB in below methods:
	// `QueryContext()` / `QueryRowContext()` / `PrepareContext()`
	ContextUsePrimaryKey contextKey = 0

	// ContextUseReplicaKey is the context key for using read replica DB in below methods:
	//
	// Comment out as seems no use case for now
	// ContextUseReplicaKey contextKey = 0
)

var emptyContextValue = struct{}{}

// WithPrimary return a copy of ctx with `ContextUsePrimaryKey` has value
func WithPrimary(ctx context.Context) context.Context {
	return context.WithValue(ctx, ContextUsePrimaryKey, emptyContextValue)
}

// context return a copy of ctx with `ContextUseReplicaKey` has value
//
// Comment out as seems no use case for now
// func WithReplica(ctx context.Context) context.Context {
// 	return context.WithValue(ctx, ContextUseReplicaKey, emptyContextValue)
// }

// UsePrimaryFromContext returns true if `ContextUsePrimaryKey` is set
// (any non-nil value is ok, better to use struct{}{} as value as it does not use memory);
// otherwise returns false
func UsePrimaryFromContext(ctx context.Context) bool {
	if val := ctx.Value(ContextUsePrimaryKey); val != nil {
		return true
	}
	return false
}

// UseReplicaFromContext returns true if `ContextUseReplicaKey` is set
// (any non-nil value is ok, better to use struct{}{} as value as it does not use memory);
// otherwise returns false
//
// Comment out as seems no use case for now
// func UseReplicaFromContext(ctx context.Context) bool {
// 	if val := ctx.Value(ContextUseReplicaKey); val != nil {
// 		return true
// 	}
// 	return false
// }

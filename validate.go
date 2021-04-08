package gosqlrwdb

import (
	"database/sql"
)

// validateNew returns an error if the invocation of `New()` is invalid
func validateNew(master *sql.DB, readreplicas ...*sql.DB) error {
	if master == nil {
		return ErrNotProvidedPrimary
	}
	if len(readreplicas) == 0 {
		return ErrNotProvidedReplicas
	}
	for _, r := range readreplicas {
		if r == nil {
			return ErrNotProvidedReplicas
		}
	}
	return nil
}

// validateQuery returns an error if the invocation of `Query()` is invalid
func validateQuery(query string, args ...interface{}) error {
	if !IsQuerySqlFunc(query) {
		return ErrNotQuerySQL
	}
	return nil
}

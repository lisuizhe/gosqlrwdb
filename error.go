package gosqlrwdb

import (
	"fmt"
)

var (
	// ErrPrimaryInMaintenance is returned when primary DB is in maintenance mode
	// but trying to use primary to do query/execution
	ErrPrimaryInMaintenance = fmt.Errorf("Primary DB is in maintenance mode")

	// ErrNotProvidedPrimary is returned when primary DB is not provided in `New()` method
	ErrNotProvidedPrimary = fmt.Errorf("Primary DB is not provided")

	// ErrNotProvidedReplicas is returned when no read replica DB is not provided in `New()` method
	ErrNotProvidedReplicas = fmt.Errorf("No replica DB is not provided")

	// ErrNotQuerySQL is returned when the provided sql is not Query SQL but used in `Query` method
	ErrNotQuerySQL = fmt.Errorf("Provided sql is not a Query SQL")

	// ErrNoReplicaAvailable is returned when no replicas is available but replica is used for query.
	//
	// Note that it WILL BE RETURNED even master is available, as we determine to fail-fast,
	// instead of defer the error until whole DB cluster overloads
	ErrNoReplicaAvailable = fmt.Errorf("No replica DB is available now")
)

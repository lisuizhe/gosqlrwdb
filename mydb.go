// package gosqlrwdb provides mechanism to automatically:
//
// - route read-only queries to read replicas
//
// - and all other queries to the primary DB
//
package gosqlrwdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/multierr"
)

type contextKey int

const (
	// EnvVarPrimaryInMaintenanceKey is to determine whether primary DB in in maintenance mode
	// when `DB` is initialized. Deploy the application using package gosqlrwdb with this
	// environment variable to `True`/`true` means master is in mainenance mode; otherwise not.
	EnvVarPrimaryInMaintenanceKey = "MYDB_PRIMARY_IN_MAINTENANCE"

	// EnvVarDebugKey is to determine whether debug information is print.
	// Deploy the application using package gosqlrwdb with this
	// environment variable to `True`/`true` means master is debug information is print; otherwise false.
	EnvVarDebugKey = "MYDB_DEBUG"

	// EnvVarDebugKey is to determine whether do validation when `New()` is called
	// (to validate at least 1 primary and 1 read replica DB are provided)
	// Deploy the application using package gosqlrwdb with this
	// environment variable to `True`/`true` means do validation when `New()` is called; otherwise not.
	EnvVarDoValidateNewKey = "MYDB_DO_VALIDATE_NEW"

	// EnvVarDisableAutoFailoverKey is to determine whether auto failover happen for read replica automatically
	// Deploy the application using package gosqlrwdb with this
	// environment variable to `True`/`true` means auto failover for read replica; otherwise not.
	EnvVarDisableReplicaAutoFailoverKey = "MYDB_DISABLE_REPLICA_AUTO_FAILOVER"
)

var (
	// Debug is to determine whether print debug information.
	// It is initialized from environment variable with key `EnvVarDebugKey`.
	// Also can update it programatically using `mydb.Debug = true`
	Debug = strings.ToLower(os.Getenv(EnvVarDebugKey)) == "true"

	// DefaultReplicaAutoFailoverInterval is used when New() to determine interval of heartbeat
	// to read replicas. Default to 30s.
	DefaultReplicaAutoFailoverInterval = 30 * time.Second

	// DisableReplicaAutoFailover is to determine whether auto failover happen for read replica automatically
	// It is initialized from environment variable with key `EnvVarDisableReplicaAutoFailoverKey`.
	// Also can update it programatically using `mydb.DisableReplicaAutoFailover = true`
	DisableReplicaAutoFailover = strings.ToLower(os.Getenv(EnvVarDisableReplicaAutoFailoverKey)) == "true"

	// DoValidateNew is to determine whether do validation when `New()` is called.
	// It is initialized from environment variable with key `EnvVarDoValidateNewKey`.
	// Also can update it programatically using `mydb.DoValidateNew = true`
	DoValidateNew = strings.ToLower(os.Getenv(EnvVarDoValidateNewKey)) == "true"

	// IsQuerySqlFunc is used to determine whether `query` in is a Query SQL
	// Overwrite IsQuerySqlFunc only when necessary
	IsQuerySqlFunc = func(query string) bool {
		if len(query) >= 6 && strings.ToLower(query[:6]) == "select" {
			return true
		}
		return false
	}
)

// debug prints debug information to std output if Debug is true
func debug(format string, a ...interface{}) {
	if Debug {
		fmt.Printf("[mydb] "+format+"\n", a...)
	}
}

var empty = struct{}{}

type DB struct {
	master              *sql.DB
	readreplicas        []*sql.DB
	count               int
	countMutex          sync.RWMutex
	needHeartbeat       bool
	unavailableReplicas map[*sql.DB]struct{}
	stopHeartbeat       chan struct{}
	primaryInMaintence  bool
}

// New returns new instance of DB.
// In case of not providing at least 1 primary DB & 1 read replica DB,
// and `DoValidateNew` is true(default is false),
// this package cannot be used correctly and will panic
func New(master *sql.DB, readreplicas ...*sql.DB) *DB {
	if DoValidateNew {
		if err := validateNew(master, readreplicas...); err != nil {
			debug("[New] err: %s", err)
			panic(err)
		}
	}

	needHeartbeat := !DisableReplicaAutoFailover
	stop := make(chan struct{})
	var unavailableReplicas = map[*sql.DB]struct{}{}
	var ticker *time.Ticker
	if needHeartbeat {
		unavailableReplicas = heartbeat(readreplicas)
		ticker = time.NewTicker(DefaultReplicaAutoFailoverInterval)
	}
	db := &DB{
		master:              master,
		readreplicas:        readreplicas,
		count:               -1, // so that start from the first read replica
		needHeartbeat:       needHeartbeat,
		unavailableReplicas: unavailableReplicas,
		stopHeartbeat:       stop,
		primaryInMaintence:  strings.ToLower(os.Getenv(EnvVarPrimaryInMaintenanceKey)) == "true",
	}
	go func() {
		for {
			select {
			case <-ticker.C:
				db.countMutex.Lock()
				db.unavailableReplicas = heartbeat(db.readreplicas)
				db.countMutex.Unlock()
			case <-stop:
				ticker.Stop()
				return
			}
		}
	}()

	return db
}

// heartbeat returns map that holds unavailable(Ping has error) readreplica
func heartbeat(readreplicas []*sql.DB) map[*sql.DB]struct{} {
	unavailableReplicas := map[*sql.DB]struct{}{}
	var err error
	for _, r := range readreplicas {
		if err = r.Ping(); err != nil {
			unavailableReplicas[r] = empty
		}
	}
	return unavailableReplicas
}

// readReplicaRoundRobin returns pointer of sql.DB to one of the read replicas,
// using Round-Robin algorithm
//
// If DisableReplicaAutoFailover is false(which is default value):
// It will automatically fail-over to another replica;
// It will returned error `ErrNoReplicaAvailable` when no read replica available.
//
// If DisableReplicaAutoFailover is true:
// replica is selected using Round-Robin algorithm and without error returned
func (db *DB) readReplicaRoundRobin(bypassAutoFailover ...bool) (*sql.DB, error) {
	if !DoValidateNew && len(db.readreplicas) == 0 {
		return nil, ErrNotProvidedReplicas
	}

	if !db.needHeartbeat || (len(bypassAutoFailover) > 0 && bypassAutoFailover[0]) {
		return db.readReplicaRoundRobinHelper(), nil
	}

	// var errs, err error
	for try := 1; try <= len(db.readreplicas); try++ {
		r := db.readReplicaRoundRobinHelper()
		if db.needHeartbeat {
			db.countMutex.RLock()
			if _, unavailable := db.unavailableReplicas[r]; unavailable {
				debug("[readReplicaRoundRobin] unavailable, try: %d", try)
				db.countMutex.RUnlock()
				continue
			} else {
				db.countMutex.RUnlock()
			}
		}
		// Comment out as we will do heartbeat every DefaultReplicaAutoFailoverInterval
		//
		// if err = r.Ping(); err != nil {
		// 	debug("[readReplicaRoundRobin] Ping err: %s, try: %d", err, try)
		// 	errs = multierr.Append(errs, err)
		// 	continue
		// }
		return r, nil
	}

	return nil, ErrNoReplicaAvailable
}

// readReplicaRoundRobinHelper returns pointer of sql.DB to one of the read replicas,
// using Round-Robin algorithm
func (db *DB) readReplicaRoundRobinHelper() *sql.DB {
	var idx int
	db.countMutex.Lock()
	db.count++
	idx = db.count % len(db.readreplicas)
	db.countMutex.Unlock()
	debug("[readReplicaRoundRobinHelper] replica idx: %d", idx)
	return db.readreplicas[idx]
}

// Ping verifies the connections to the primary &read replicas are still alive,
// returns error if any is not alive
func (db *DB) Ping() error {
	if !DoValidateNew {
		if db.master == nil {
			return ErrNotProvidedPrimary
		}
		if len(db.readreplicas) == 0 {
			return ErrNotProvidedReplicas
		}
	}

	if !db.primaryInMaintence {
		if err := db.master.Ping(); err != nil {
			debug("[Ping] master err: %s", err)
			return err
		}
	}

	for i := range db.readreplicas {
		if err := db.readreplicas[i].Ping(); err != nil {
			debug("[Ping] readreplicas[%d] err: %s", i, err)
			return err
		}
	}

	return nil
}

// PingContext verifies the connections to the primary &read replicas are still alive,
// returns error if any is not alive
func (db *DB) PingContext(ctx context.Context) error {
	if !DoValidateNew {
		if db.master == nil {
			return ErrNotProvidedPrimary
		}
		if len(db.readreplicas) == 0 {
			return ErrNotProvidedReplicas
		}
	}

	if !db.primaryInMaintence {
		if err := db.master.PingContext(ctx); err != nil {
			debug("[PingContext] master err: %s", err)
			return err
		}
	}

	for i := range db.readreplicas {
		if err := db.readreplicas[i].PingContext(ctx); err != nil {
			debug("[PingContext] readreplicas[%d] err: %s", i, err)
			return err
		}
	}

	return nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
//
// Internally it uses one of read replica DB.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var err error
	if err = validateQuery(query, args...); err != nil {
		debug("[Query] validate err: %s", err)
		return nil, err
	}
	var tgtdb *sql.DB
	if tgtdb, err = db.readReplicaRoundRobin(); err != nil {
		debug("[Query] readReplicaRoundRobin err: %s", err)
		return nil, err
	}
	return tgtdb.Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
//
// Internally it uses one of read replica DB normally;
// in case that `ctx` is created from `mydb.WithPrimary(ctx)`,
// or have `ContextUsePrimaryKey` in context value, it will use primary DB
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var err error
	if err := validateQuery(query, args...); err != nil {
		debug("[QueryContext] validate err: %s", err)
		return nil, err
	}

	var tgtdb *sql.DB
	if usePrimary := UsePrimaryFromContext(ctx); usePrimary && !db.primaryInMaintence {
		if !DoValidateNew && db.master == nil {
			debug("[QueryContext] primary err: %s", ErrNotProvidedPrimary)
			return nil, ErrNotProvidedPrimary
		}
		tgtdb = db.master
	} else {
		if tgtdb, err = db.readReplicaRoundRobin(); err != nil {
			debug("[QueryContext] readReplicaRoundRobin err: %s", err)
			return nil, err
		}
	}
	return tgtdb.QueryContext(ctx, query, args...)
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
//
// Internally it uses one of read replica DB.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	var err error
	tgtdb, err := db.readReplicaRoundRobin(true)
	if err != nil {
		debug("[QueryContext] readReplicaRoundRobin err: %s", err)
		panic(err)
	}
	return tgtdb.QueryRow(query, args...)
}

// QueryRowContext executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
//
// Internally it uses one of read replica DB normally;
// in case that `ctx` is created from `mydb.WithPrimary(ctx)`,
// or have `ContextUsePrimaryKey` in context value, it will use primary DB
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	var tgtdb *sql.DB
	if usePrimary := UsePrimaryFromContext(ctx); usePrimary && !db.primaryInMaintence {
		if !DoValidateNew && db.master == nil {
			debug("[QueryRowContext] primary err: %s", ErrNotProvidedPrimary)
			panic(ErrNotProvidedPrimary)
		}
		tgtdb = db.master
	} else {
		var err error
		tgtdb, err = db.readReplicaRoundRobin(true)
		if err != nil {
			debug("[QueryRowContext] readReplicaRoundRobin err: %s", err)
			panic(err)
		}
	}
	return tgtdb.QueryRowContext(ctx, query, args...)
}

// Begin starts a transaction.
// The default isolation level is dependent on the driver.
//
// Internally it uses primary DB.
func (db *DB) Begin() (*sql.Tx, error) {
	if db.primaryInMaintence {
		debug("[Begin] err: %s", ErrPrimaryInMaintenance)
		return nil, ErrPrimaryInMaintenance
	}
	if !DoValidateNew && db.master == nil {
		debug("[Begin] err: %s", ErrNotProvidedPrimary)
		return nil, ErrNotProvidedPrimary
	}
	return db.master.Begin()
}

// BeginTx starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back the transaction.
// Tx.Commit will return an error if the context provided to BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support, an error will be returned.
//
// Internally it uses primary DB.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if db.primaryInMaintence {
		debug("[BeginTx] err: %s", ErrPrimaryInMaintenance)
		return nil, ErrPrimaryInMaintenance
	}
	if !DoValidateNew && db.master == nil {
		debug("[BeginTx] err: %s", ErrNotProvidedPrimary)
		return nil, ErrNotProvidedPrimary
	}
	return db.master.BeginTx(ctx, opts)
}

// Close closes the primary & read replicas DB
func (db *DB) Close() error {
	close(db.stopHeartbeat)
	var errs, err error
	if !db.primaryInMaintence {
		if err = db.master.Close(); err != nil {
			errs = multierr.Append(errs, err)
		}
	}

	for i := range db.readreplicas {
		if err = db.readreplicas[i].Close(); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	if errs != nil {
		debug("[Close] err: %s", errs)
	}
	return errs
}

// Exec executes a query without returning any rows. The args are for any placeholder parameters in the query.
//
// Internally it uses primary DB.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db.primaryInMaintence {
		debug("[Exec] err: %s", ErrPrimaryInMaintenance)
		return nil, ErrPrimaryInMaintenance
	}
	if !DoValidateNew && db.master == nil {
		debug("[Exec] err: %s", ErrNotProvidedPrimary)
		return nil, ErrNotProvidedPrimary
	}
	return db.master.Exec(query, args...)
}

// ExecContext executes a query without returning any rows. The args are for any placeholder parameters in the query.
//
// Internally it uses primary DB.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.primaryInMaintence {
		debug("[ExecContext] err: %s", ErrPrimaryInMaintenance)
		return nil, ErrPrimaryInMaintenance
	}
	if !DoValidateNew && db.master == nil {
		debug("[ExecContext] err: %s", ErrNotProvidedPrimary)
		return nil, ErrNotProvidedPrimary
	}
	return db.master.ExecContext(ctx, query, args...)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned statement.
// The caller must call the statement's Close method when the statement is no longer needed.
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	var err error
	var tgtdb *sql.DB
	if isQuery := IsQuerySqlFunc(query); isQuery {
		if tgtdb, err = db.readReplicaRoundRobin(); err != nil {
			debug("[Prepare] err: %s", err)
			return nil, err
		}
	} else {
		if db.primaryInMaintence {
			debug("[Prepare] err: %s", ErrPrimaryInMaintenance)
			return nil, ErrPrimaryInMaintenance
		}
		if !DoValidateNew && db.master == nil {
			debug("[Prepare] err: %s", ErrNotProvidedPrimary)
			return nil, ErrNotProvidedPrimary
		}
		tgtdb = db.master
	}
	return tgtdb.Prepare(query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned statement.
// The caller must call the statement's Close method when the statement is no longer needed.
func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var err error
	isQuery := IsQuerySqlFunc(query)
	usePrimary := UsePrimaryFromContext(ctx)
	var tgtdb *sql.DB
	if isQuery && !usePrimary {
		if tgtdb, err = db.readReplicaRoundRobin(); err != nil {
			debug("[PrepareContext] err: %s", ErrNotProvidedPrimary)
			return nil, err
		}
	} else {
		if db.primaryInMaintence {
			debug("[PrepareContext] err: %s", ErrPrimaryInMaintenance)
			return nil, ErrPrimaryInMaintenance
		}
		if !DoValidateNew && db.master == nil {
			debug("[PrepareContext] err: %s", ErrNotProvidedPrimary)
			return nil, ErrNotProvidedPrimary
		}
		tgtdb = db.master
	}
	return tgtdb.PrepareContext(ctx, query)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	if !DoValidateNew {
		if db.master == nil {
			debug("[SetConnMaxLifetime] err: %s", ErrNotProvidedPrimary)
			panic(ErrNotProvidedPrimary)
		}
		if len(db.readreplicas) == 0 {
			debug("[SetConnMaxLifetime] err: %s", ErrNotProvidedReplicas)
			panic(ErrNotProvidedReplicas)
		}
	}

	if !db.primaryInMaintence {
		debug("[SetConnMaxLifetime] master: %s", d)
		db.master.SetConnMaxLifetime(d)
	}

	for i := range db.readreplicas {
		debug("[SetConnMaxLifetime] replica[%d]: %s", i, d)
		db.readreplicas[i].SetConnMaxLifetime(d)
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
func (db *DB) SetMaxIdleConns(n int) {
	if !DoValidateNew {
		if db.master == nil {
			debug("[SetMaxIdleConns] err: %s", ErrNotProvidedPrimary)
			panic(ErrNotProvidedPrimary)
		}
		if len(db.readreplicas) == 0 {
			debug("[SetMaxIdleConns] err: %s", ErrNotProvidedReplicas)
			panic(ErrNotProvidedReplicas)
		}
	}

	if !db.primaryInMaintence {
		debug("[SetMaxIdleConns] master: %d", n)
		db.master.SetMaxIdleConns(n)
	}

	for i := range db.readreplicas {
		debug("[SetMaxIdleConns] replica[%d]: %d", i, n)
		db.readreplicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
func (db *DB) SetMaxOpenConns(n int) {
	if !DoValidateNew {
		if db.master == nil {
			debug("[SetMaxOpenConns] err: %s", ErrNotProvidedPrimary)
			panic(ErrNotProvidedPrimary)
		}
		if len(db.readreplicas) == 0 {
			debug("[SetMaxOpenConns] err: %s", ErrNotProvidedReplicas)
			panic(ErrNotProvidedReplicas)
		}
	}

	if !db.primaryInMaintence {
		debug("[SetMaxOpenConns] master: %d", n)
		db.master.SetMaxOpenConns(n)
	}

	for i := range db.readreplicas {
		debug("[SetMaxOpenConns] replica[%d]: %d", i, n)
		db.readreplicas[i].SetMaxOpenConns(n)
	}
}

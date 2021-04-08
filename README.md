# gosqlrwdb

`gosqlrwdb` provides mechanism to automatically:
- route read-only queries to read replicas
- and all other queries to the primary DB

## Installation

```sh
go get -u github.com/lisuizhe/gosqlrwdb
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
     _ "github.com/go-sql-driver/mysql" // Using MySQL driver
    "flag"
    "log"
    "os"
    "os/signal"
    "time"
    "github.com/lisuizhe/gosqlrwdb"
)

var pool *mydb.DB // Database connection pool.

func main() {
    id := flag.Int64("id", 0, "person ID to find")
    pdsn := flag.String("dsn0", os.Getenv("DSN_PRIMARY"), "connection primary data source name")
    var rdsns = []string{
        flag.String("dsn1", os.Getenv("DSN_REPLICA_1"), "connection replica data source 1 name")
        flag.String("dsn2", os.Getenv("DSN_REPLICA_2"), "connection replica data source 2 name")
    }
    flag.Parse()

    if len(*pdsn) == 0 {
        log.Fatal("missing dsn0 flag")
    }
    for i, rdsn := range rdsns {
        if len(*rdsn) == 0 {
            log.Fatalf("missing dsn%d flag", i)
        }
    }
    if *id == 0 {
        log.Fatal("missing person ID")
    }

    var err error
    // Opening a driver typically will not attempt to connect to the database.
    primaryDB, err := sql.Open("mysql", *pdsn)
    if err != nil {
        // This will not be a connection error, but a DSN parse error or
        // another initialization error.
        log.Fatalf("unable to use data source name: %v, err: %v", *pdsn, err)
    }
    var replicaDBs = []*sql.DB{}
    for _, rdsn := range rdsns {
        replicaDB, err := sql.Open("mysql", *rdsn)
        if err != nil {
            log.Fatalf("unable to use data source name: %v, err: %v", *rdsn, err)
        }
    }
    pool = mydb.New(primaryDB, replicaDBs...)
    defer pool.Close()

    pool.SetConnMaxLifetime(0)
    pool.SetMaxIdleConns(3)
    pool.SetMaxOpenConns(3)

    ctx, stop := context.WithCancel(context.Background())
    defer stop()

    appSignal := make(chan os.Signal, 3)
    signal.Notify(appSignal, os.Interrupt)

    go func() {
        select {
        case <-appSignal:
            stop()
        }
    }()

    Ping(ctx)

    Query(ctx, *id)

    Delete(ctx, *id)
}

// Ping the database to verify DSN provided by the user is valid and the
// server accessible. If the ping fails exit the program with an error.
func Ping(ctx context.Context) {
    ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
    defer cancel()

    if err := pool.PingContext(ctx); err != nil {
        log.Fatalf("unable to connect to database: %v", err)
    }
}

// Query the database for the information requested and prints the results.
// If the query fails exit the program with an error.
func Query(ctx context.Context, id int64) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    var name string
    err := pool.QueryRowContext(ctx, "select p.name from people as p where p.id = :id;", sql.Named("id", id)).Scan(&name)
    if err != nil {
        log.Fatal("unable to execute search query", err)
    }
    log.Println("name=", name)
}

// Delete deletes the database for the information requested and prints the results.
// If the query fails exit the program with an error.
func Delete(ctx context.Context, id int64) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    _, err := pool.ExecContext(ctx, "delete from people as p where p.id = :id;", sql.Named("id", id))
    if err != nil {
        log.Fatal("unable to execute search query", err)
    }
}
```

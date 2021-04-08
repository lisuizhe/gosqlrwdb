package gosqlrwdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

type mydbMock struct {
	db   *sql.DB
	mock sqlmock.Sqlmock
}

const (
	selectQueryTmpl  = "select %s from mytable"
	insertQueryTmpl  = "insert into mytable %s"
	updateQueryTmpl  = "update mytable %s"
	deleteQuueryTmpl = "delete from mytable %s"
)

// options[0] = true => MonitorPingsOption(true)
func newMydbMock(options ...bool) (*mydbMock, error) {
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error
	if len(options) == 0 {
		db, mock, err = sqlmock.New()
	} else if options[0] {
		db, mock, err = sqlmock.New(sqlmock.MonitorPingsOption(true))
	}

	if err != nil {
		return nil, err
	}
	return &mydbMock{
		db:   db,
		mock: mock,
	}, nil
}

func TestNew(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	var db *DB
	db = New(p.db, r1.db)
	if db == nil {
		t.Errorf("New() return nil DB for 1 primary, 1 replica")
	}
	db.Close()
	db = New(p.db, r1.db, r2.db)
	if db == nil {
		t.Errorf("New() return nil DB for 1 primary, 2 replica")
	}
	db.Close()
}

func TestNewPanicWhenNoPrimary(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic but should panic when no primary")
		}
		DoValidateNew = false
	}()

	DoValidateNew = true
	db := New((*sql.DB)(nil), []*sql.DB{}...)
	db.Close()
}

func TestNewPanicWhenNoReplica(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic but should panic when no primary")
		}
		DoValidateNew = false
	}()

	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	DoValidateNew = true
	db := New(p.db, []*sql.DB{}...)
	db.Close()
}

func TestNewDefaultPrimaryInMaintence(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	db := New(p.db, r1.db, r2.db)
	defer db.Close()
	if db.primaryInMaintence {
		t.Errorf("actual primaryInMaintence: %t, expected false", db.primaryInMaintence)
	}
}

func TestNewConfigurePrimaryInMaintence(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	os.Setenv(EnvVarPrimaryInMaintenanceKey, "true")
	db := New(p.db, r1.db, r2.db)
	defer db.Close()
	if !db.primaryInMaintence {
		t.Errorf("actual primaryInMaintence: %t, expected true", db.primaryInMaintence)
	}
	os.Setenv(EnvVarPrimaryInMaintenanceKey, "")
}

func TestDefaultVarFromEnv(t *testing.T) {
	if Debug {
		t.Errorf("actual Debug: %t, expected false", Debug)
	}
	if DisableReplicaAutoFailover {
		t.Errorf("actual DisableReplicaAutoFailover: %t, expected false", Debug)
	}
	if DoValidateNew {
		t.Errorf("actual DoValidateNew: %t, expected false", Debug)
	}
}

func TestDefaultIsQuerySqlFunc(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"select * from mytable", true},
		{"insert into mytable values (1, 1)", false},
		{"update mytable set column2 = 2 where column1 = 1", false},
		{"delete from mytable where column1 = 1", false},
	}

	for _, test := range tests {
		actual := IsQuerySqlFunc(test.query)
		if actual != test.expected {
			t.Errorf("actual: %t, expected %t, query: %s", actual, test.expected, test.query)
		}
	}
}

func TestConfigureIsQuerySqlFunc(t *testing.T) {
	queries := []string{
		"select * from mytable",
		"insert into mytable values (1, 1)",
		"update mytable set column2 = 2 where column1 = 1",
		"delete from mytable where column1 = 1",
	}

	defaultIsQuerySqlFunc := IsQuerySqlFunc

	expected := false
	IsQuerySqlFunc = func(_ string) bool {
		return expected
	}
	for _, q := range queries {
		actual := IsQuerySqlFunc(q)
		if actual != expected {
			t.Errorf("actual: %t, expected %t, query: %s", actual, expected, q)
		}
	}

	expected = true
	IsQuerySqlFunc = func(_ string) bool {
		return expected
	}
	for _, q := range queries {
		actual := IsQuerySqlFunc(q)
		if actual != expected {
			t.Errorf("actual: %t, expected %t, query: %s", actual, expected, q)
		}
	}

	IsQuerySqlFunc = defaultIsQuerySqlFunc
}

func TestReadReplicaRoundRobinHelper1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	db := New(p.db, r1.db, r2.db)
	defer db.Close()
	testReplicaIndexMatch(t, db, 0, true)
	testReplicaIndexMatch(t, db, 1, true)
	testReplicaIndexMatch(t, db, 0, true)
	testReplicaIndexMatch(t, db, 1, true)
}

func TestReadReplicaRoundRobinHelper2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r3, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	db := New(p.db, r1.db, r2.db, r3.db)
	defer db.Close()
	testReplicaIndexMatch(t, db, 0, true)
	testReplicaIndexMatch(t, db, 1, true)
	testReplicaIndexMatch(t, db, 2, true)
	testReplicaIndexMatch(t, db, 0, true)
	testReplicaIndexMatch(t, db, 1, true)
	testReplicaIndexMatch(t, db, 2, true)
}

func TestReadReplicaRoundRobin1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	db := New(p.db, r1.db, r2.db)
	defer db.Close()
	testReplicaIndexMatch(t, db, 0)
	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 0)
	testReplicaIndexMatch(t, db, 1)
}

func TestReadReplicaRoundRobin2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r3, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	db := New(p.db, r1.db, r2.db, r3.db)
	defer db.Close()
	testReplicaIndexMatch(t, db, 0)
	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 2)
	testReplicaIndexMatch(t, db, 0)
	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 2)
}

// skip this as the failover logic change
func SkipTestReadReplicaRoundRobinAutoFailoverLegacy(t *testing.T) {
	var err error
	p, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r3, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	db := New(p.db, r1.db, r2.db, r3.db)
	defer db.Close()
	r1.mock.ExpectPing().WillReturnError(fmt.Errorf("Not available"))
	r2.mock.ExpectPing()
	r3.mock.ExpectPing()
	r1.mock.ExpectPing().WillReturnError(fmt.Errorf("Not available"))
	r2.mock.ExpectPing()
	r3.mock.ExpectPing()

	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 2)
	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 2)
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r3.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestReadReplicaRoundRobinAutoFailover(t *testing.T) {
	var err error
	p, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r3, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}

	r1.mock.ExpectPing().WillReturnError(fmt.Errorf("Not available"))
	r2.mock.ExpectPing()
	r3.mock.ExpectPing()
	db := New(p.db, r1.db, r2.db, r3.db)
	defer db.Close()

	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 2)
	testReplicaIndexMatch(t, db, 1)
	testReplicaIndexMatch(t, db, 2)
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r3.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPing(t *testing.T) {
	var err error
	p, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	p.mock.ExpectPing()
	r1.mock.ExpectPing()
	r2.mock.ExpectPing()
	if err = db.Ping(); err != nil {
		t.Errorf("error %s when Ping", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPingContext(t *testing.T) {
	var err error
	p, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock(true)
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	p.mock.ExpectPing()
	r1.mock.ExpectPing()
	r2.mock.ExpectPing()
	if err = db.PingContext(context.Background()); err != nil {
		t.Errorf("error %s when PingContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQuery(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")

	r1.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)

	if _, err = db.Query(fmt.Sprintf(selectQueryTmpl, "*")); err != nil {
		t.Errorf("error %s when Query", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	r2.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	if _, err = db.Query(fmt.Sprintf(selectQueryTmpl, "*")); err != nil {
		t.Errorf("error %s when Query", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryInvalid(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	if _, err = db.Query(fmt.Sprintf(insertQueryTmpl, "values (1, '1')")); err != ErrNotQuerySQL {
		t.Errorf("error [%s] when Query, expected [%s]", err, ErrNotQuerySQL)
	}
	if _, err = db.Query(fmt.Sprintf(updateQueryTmpl, "set column2 = ? where column1 = ?"), "2", 1); err != ErrNotQuerySQL {
		t.Errorf("error [%s] when Query, expected [%s]", err, ErrNotQuerySQL)
	}
	if _, err = db.Query(fmt.Sprintf(deleteQuueryTmpl, "")); err != ErrNotQuerySQL {
		t.Errorf("error [%s] when Query, expected [%s]", err, ErrNotQuerySQL)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryContext(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")

	r1.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	if _, err = db.QueryContext(context.Background(), fmt.Sprintf(selectQueryTmpl, "*")); err != nil {
		t.Errorf("error %s when QueryContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	r2.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	if _, err = db.QueryContext(context.Background(), fmt.Sprintf(selectQueryTmpl, "*")); err != nil {
		t.Errorf("error %s when QueryContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryContextInvalid(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	if _, err = db.QueryContext(context.Background(), fmt.Sprintf(insertQueryTmpl, "values (1, '1')")); err != ErrNotQuerySQL {
		t.Errorf("error [%s] when QueryContext, expected [%s]", err, ErrNotQuerySQL)
	}
	if _, err = db.QueryContext(context.Background(), fmt.Sprintf(updateQueryTmpl, "set column2 = ? where column1 = ?"), "2", 1); err != ErrNotQuerySQL {
		t.Errorf("error [%s] when QueryContext, expected [%s]", err, ErrNotQuerySQL)
	}
	if _, err = db.QueryContext(context.Background(), fmt.Sprintf(deleteQuueryTmpl, "")); err != ErrNotQuerySQL {
		t.Errorf("error [%s] when QueryContext, expected [%s]", err, ErrNotQuerySQL)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryContextUsePrimary(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")

	p.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	if _, err = db.QueryContext(WithPrimary(context.Background()), fmt.Sprintf(selectQueryTmpl, "*")); err != nil {
		t.Errorf("error %s when QueryContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryRow1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	var col1 int
	var col2 string

	r1.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	rows := db.QueryRow(fmt.Sprintf(selectQueryTmpl, "*"))
	if err = rows.Scan(&col1, &col2); err != nil {
		t.Errorf("error %s when QueryRow", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryRow2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	var col1 int
	var col2 string

	r2.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	db.readReplicaRoundRobin()
	rows := db.QueryRow(fmt.Sprintf(selectQueryTmpl, "*"))
	if err = rows.Scan(&col1, &col2); err != nil {
		t.Errorf("error %s when QueryRow", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryRowContext(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	var col1 int
	var col2 string

	r1.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	rows := db.QueryRowContext(context.Background(), fmt.Sprintf(selectQueryTmpl, "*"))
	if err = rows.Scan(&col1, &col2); err != nil {
		t.Errorf("error %s when QueryRowContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	r2.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	if _, err = db.QueryContext(context.Background(), fmt.Sprintf(selectQueryTmpl, "*")); err != nil {
		t.Errorf("error %s when QueryContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryRowContextUsePrimary(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	var col1 int
	var col2 string

	p.mock.ExpectQuery(fmt.Sprintf(selectQueryTmpl, "(.+)")).WillReturnRows(mrows)
	rows := db.QueryRowContext(WithPrimary(context.Background()), fmt.Sprintf(selectQueryTmpl, "*"))
	if err = rows.Scan(&col1, &col2); err != nil {
		t.Errorf("error %s when QueryRowContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBegin1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	p.mock.ExpectBegin()
	p.mock.ExpectCommit()
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("error %s when Begin", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Errorf("error %s when Commit", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBegin2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	p.mock.ExpectBegin()
	p.mock.ExpectRollback()
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("error %s when Begin", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Errorf("error %s when Rollback", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBeginTx1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	p.mock.ExpectBegin()
	p.mock.ExpectCommit()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Errorf("error %s when BeginTx", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Errorf("error %s when Commit", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBeginTx2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	p.mock.ExpectBegin()
	p.mock.ExpectRollback()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Errorf("error %s when BeginTx", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Errorf("error %s when Rollback", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestClose(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)

	p.mock.ExpectClose()
	r1.mock.ExpectClose()
	r2.mock.ExpectClose()
	db.Close()
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestExec(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mresult := sqlmock.NewResult(2, 1)
	p.mock.ExpectExec(fmt.Sprintf(insertQueryTmpl, "")).WillReturnResult(mresult)
	_, err = db.Exec(fmt.Sprintf(insertQueryTmpl, "values (?, ?)"), 2, "2")
	if err != nil {
		t.Errorf("error %s when QueryRowContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestExecWhenPrimaryInMaintenance(t *testing.T) {
	os.Setenv(EnvVarPrimaryInMaintenanceKey, "true")
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer func() {
		db.Close()
		os.Setenv(EnvVarPrimaryInMaintenanceKey, "")
	}()

	_, err = db.Exec(fmt.Sprintf(insertQueryTmpl, "values (?, ?)"), 2, "2")
	if err != ErrPrimaryInMaintenance {
		t.Errorf("error [%s] when QueryRowContext, expected [%s]", err, ErrPrimaryInMaintenance)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestExecContext(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mresult := sqlmock.NewResult(2, 1)
	p.mock.ExpectExec(fmt.Sprintf(insertQueryTmpl, "")).WillReturnResult(mresult)
	_, err = db.ExecContext(context.Background(), fmt.Sprintf(insertQueryTmpl, "values (?, ?)"), 2, "2")
	if err != nil {
		t.Errorf("error %s when QueryRowContext", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestExecContextWhenPrimaryInMaintenance(t *testing.T) {
	os.Setenv(EnvVarPrimaryInMaintenanceKey, "true")
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer func() {
		db.Close()
		os.Setenv(EnvVarPrimaryInMaintenanceKey, "")
	}()

	_, err = db.ExecContext(context.Background(), fmt.Sprintf(insertQueryTmpl, "values (?, ?)"), 2, "2")
	if err != ErrPrimaryInMaintenance {
		t.Errorf("error [%s] when QueryRowContext, expected [%s]", err, ErrPrimaryInMaintenance)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareWithSelect1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	r1.mock.ExpectPrepare(fmt.Sprintf(selectQueryTmpl, "")).ExpectQuery().WillReturnRows(mrows)
	stmt, err := db.Prepare(fmt.Sprintf(selectQueryTmpl, ""))
	if err != nil {
		t.Errorf("error %s when Prepare", err)
	}
	if _, err = stmt.Query(); err != nil {
		t.Errorf("error %s when stmt.Query", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareWithSelect2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	db.readReplicaRoundRobin()
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	r2.mock.ExpectPrepare(fmt.Sprintf(selectQueryTmpl, "")).ExpectQuery().WillReturnRows(mrows)
	stmt, err := db.Prepare(fmt.Sprintf(selectQueryTmpl, ""))
	if err != nil {
		t.Errorf("error %s when Prepare", err)
	}
	if _, err = stmt.Query(); err != nil {
		t.Errorf("error %s when stmt.Query", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareWithInsert(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mresult := sqlmock.NewResult(2, 1)
	p.mock.ExpectPrepare(fmt.Sprintf(insertQueryTmpl, "")).ExpectExec().WillReturnResult(mresult)
	stmt, err := db.Prepare(fmt.Sprintf(insertQueryTmpl, "values (?, ?)"))
	if err != nil {
		t.Errorf("error %s when Prepare", err)
	}
	if _, err = stmt.Exec(2, "2"); err != nil {
		t.Errorf("error %s when stmt.Exec", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareWithInsertWhenPrimaryInMaintenance(t *testing.T) {
	os.Setenv(EnvVarPrimaryInMaintenanceKey, "true")
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer func() {
		db.Close()
		os.Setenv(EnvVarPrimaryInMaintenanceKey, "")
	}()

	_, err = db.Prepare(fmt.Sprintf(insertQueryTmpl, "values (?, ?)"))
	if err != ErrPrimaryInMaintenance {
		t.Errorf("error [%s] when Prepare, expected [%s]", err, ErrPrimaryInMaintenance)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareContextWithSelect1(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	r1.mock.ExpectPrepare(fmt.Sprintf(selectQueryTmpl, "")).ExpectQuery().WillReturnRows(mrows)
	stmt, err := db.PrepareContext(context.Background(), fmt.Sprintf(selectQueryTmpl, ""))
	if err != nil {
		t.Errorf("error %s when PrepareContext", err)
	}
	if _, err = stmt.Query(); err != nil {
		t.Errorf("error %s when stmt.Query", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareContextWithSelect2(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	db.readReplicaRoundRobin()
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	r2.mock.ExpectPrepare(fmt.Sprintf(selectQueryTmpl, "")).ExpectQuery().WillReturnRows(mrows)
	stmt, err := db.PrepareContext(context.Background(), fmt.Sprintf(selectQueryTmpl, ""))
	if err != nil {
		t.Errorf("error %s when PrepareContext", err)
	}
	if _, err = stmt.Query(); err != nil {
		t.Errorf("error %s when stmt.Query", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareContextWithSelectUsePrimary(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mrows := sqlmock.NewRows([]string{"column1", "column2"}).AddRow(1, "1")
	p.mock.ExpectPrepare(fmt.Sprintf(selectQueryTmpl, "")).ExpectQuery().WillReturnRows(mrows)
	stmt, err := db.PrepareContext(WithPrimary(context.Background()), fmt.Sprintf(selectQueryTmpl, ""))
	if err != nil {
		t.Errorf("error %s when PrepareContext", err)
	}
	if _, err = stmt.Query(); err != nil {
		t.Errorf("error %s when stmt.Query", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareContextWithInsert(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer db.Close()

	mresult := sqlmock.NewResult(2, 1)
	p.mock.ExpectPrepare(fmt.Sprintf(insertQueryTmpl, "")).ExpectExec().WillReturnResult(mresult)
	stmt, err := db.PrepareContext(context.Background(), fmt.Sprintf(insertQueryTmpl, "values (?, ?)"))
	if err != nil {
		t.Errorf("error %s when PrepareContext", err)
	}
	if _, err = stmt.Exec(2, "2"); err != nil {
		t.Errorf("error %s when stmt.Exec", err)
	}
	if err = stmt.Close(); err != nil {
		t.Errorf("error %s when stmt.Close", err)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPrepareContextWithInsertWhenPrimaryInMaintenance(t *testing.T) {
	os.Setenv(EnvVarPrimaryInMaintenanceKey, "true")
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	defer func() {
		db.Close()
		os.Setenv(EnvVarPrimaryInMaintenanceKey, "")
	}()

	_, err = db.PrepareContext(context.Background(), fmt.Sprintf(insertQueryTmpl, "values (?, ?)"))
	if err != ErrPrimaryInMaintenance {
		t.Errorf("error [%s] when PrepareContext, expected [%s]", err, ErrPrimaryInMaintenance)
	}
	if err = p.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r1.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	if err = r2.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSetConnMaxLifetime(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	Debug = true
	defer func() {
		db.Close()
		Debug = false
	}()

	db.SetConnMaxLifetime(1 * time.Minute)
	p.mock.ExpectClose()
	r1.mock.ExpectClose()
	r2.mock.ExpectClose()
}

func TestSetMaxIdleConns(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	Debug = true
	defer func() {
		db.Close()
		Debug = false
	}()

	db.SetMaxIdleConns(10)
	p.mock.ExpectClose()
	r1.mock.ExpectClose()
	r2.mock.ExpectClose()
}

func TestSetMaxOpenConns(t *testing.T) {
	var err error
	p, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r1, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	r2, err := newMydbMock()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	db := New(p.db, r1.db, r2.db)
	Debug = true
	defer func() {
		db.Close()
		Debug = false
	}()

	db.SetMaxOpenConns(100)
	p.mock.ExpectClose()
	r1.mock.ExpectClose()
	r2.mock.ExpectClose()
}

func replicaIndex(readreplicas []*sql.DB, replica *sql.DB) int {
	for i := range readreplicas {
		if readreplicas[i] == replica {
			return i
		}
	}
	return -1
}

func testReplicaIndexMatch(t *testing.T, db *DB, expectedIdx int, testHelper ...bool) {
	var r *sql.DB
	if len(testHelper) > 0 && testHelper[0] {
		r = db.readReplicaRoundRobinHelper()
	} else {
		r, _ = db.readReplicaRoundRobin()
	}

	rIdx := replicaIndex(db.readreplicas, r)
	if rIdx != expectedIdx {
		t.Errorf("actual replica index: %d, expected: %d", rIdx, expectedIdx)
	}
}

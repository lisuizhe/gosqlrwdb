package gosqlrwdb

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestValidateNew(t *testing.T) {
	pdb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	rdb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error %s when creating mock databasen", err)
	}
	tests := []struct {
		master       *sql.DB
		readreplicas []*sql.DB
		expected     error
	}{
		{pdb, []*sql.DB{rdb}, nil},
		{(*sql.DB)(nil), []*sql.DB{rdb}, ErrNotProvidedPrimary},
		{pdb, []*sql.DB{}, ErrNotProvidedReplicas},
	}

	for _, test := range tests {
		actual := validateNew(test.master, test.readreplicas...)
		if actual != test.expected {
			t.Errorf("actual = %v, expected = %v", actual, test.expected)
		}
	}
}

func TestValidateQuery(t *testing.T) {
	tests := []struct {
		query    string
		args     []interface{}
		expected error
	}{
		{"select * from mytable", []interface{}{}, nil},
		{"delete from mytable", []interface{}{}, ErrNotQuerySQL},
	}

	for _, test := range tests {
		actual := validateQuery(test.query, test.args...)
		if actual != test.expected {
			t.Errorf("actual = %v, expected = %v", actual, test.expected)
		}
	}
}

package gosqlrwdb

import (
	"context"
	"testing"
)

func TestWithPrimary(t *testing.T) {
	tests := []struct {
		ctx      context.Context
		expected bool
	}{
		{context.Background(), false},
		{WithPrimary(context.Background()), true},
	}

	for _, test := range tests {
		val := test.ctx.Value(ContextUsePrimaryKey)
		actual := false
		if val != nil {
			actual = true
		}
		if actual != test.expected {
			t.Errorf("actual = %v, expected = %v, val = %v", actual, test.expected, val)
		}
	}
}

func TestUsePrimaryFromContext(t *testing.T) {
	tests := []struct {
		ctx      context.Context
		expected bool
	}{
		{context.Background(), false},
		{WithPrimary(context.Background()), true},
	}

	for _, test := range tests {
		actual := UsePrimaryFromContext(test.ctx)
		if actual != test.expected {
			t.Errorf("actual = %v, expected = %v", actual, test.expected)
		}
	}
}

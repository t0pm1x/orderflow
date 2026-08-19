package outbox

import (
	"strings"
	"testing"
)

// TestSQLFragments_NonEmpty ensures the embedded SQL files were
// actually populated. A missing embed shows up as an empty string
// here, so this is a cheap guardrail for the embed directives.
func TestSQLFragments_NonEmpty(t *testing.T) {
	cases := map[string]string{
		"insert":       insertSQL,
		"fetchPending": fetchPendingSQL,
		"markSent":     markSentSQL,
		"markFailed":   markFailedSQL,
	}
	for name, s := range cases {
		if strings.TrimSpace(s) == "" {
			t.Errorf("embedded SQL %q is empty (missing .sql file or bad //go:embed)", name)
		}
	}
}

// TestInsertSQL_MentionsTable ensures the writer's INSERT targets
// the right table name. Catches a rename that forgets to update
// either side.
func TestInsertSQL_MentionsTable(t *testing.T) {
	if !strings.Contains(insertSQL, "order_outbox") {
		t.Errorf("insertSQL does not mention order_outbox:\n%s", insertSQL)
	}
}

// TestFetchPendingSQL_OrdersByCreatedAt pins the poller's expected
// ordering (FIFO by creation time).
func TestFetchPendingSQL_OrdersByCreatedAt(t *testing.T) {
	if !strings.Contains(strings.ToLower(fetchPendingSQL), "order by created_at") {
		t.Errorf("fetchPendingSQL missing ORDER BY created_at:\n%s", fetchPendingSQL)
	}
	if !strings.Contains(fetchPendingSQL, "status = 'PENDING'") &&
		!strings.Contains(fetchPendingSQL, "status='PENDING'") {
		t.Errorf("fetchPendingSQL missing status filter:\n%s", fetchPendingSQL)
	}
	if !strings.Contains(strings.ToUpper(fetchPendingSQL), "FOR UPDATE SKIP LOCKED") {
		t.Errorf("fetchPendingSQL missing FOR UPDATE SKIP LOCKED (regression risk: two pollers would publish the same row):\n%s", fetchPendingSQL)
	}
}

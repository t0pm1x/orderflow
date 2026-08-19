package outbox

import (
	"strings"
	"testing"
)

func TestSQLFragments_NonEmpty(t *testing.T) {
	cases := map[string]string{
		"insert":       insertSQL,
		"fetchPending": fetchPendingSQL,
		"markSent":     markSentSQL,
		"markFailed":   markFailedSQL,
	}
	for name, s := range cases {
		if strings.TrimSpace(s) == "" {
			t.Errorf("embedded SQL %q is empty", name)
		}
	}
}

func TestInsertSQL_MentionsTable(t *testing.T) {
	if !strings.Contains(insertSQL, "payment_outbox") {
		t.Errorf("insertSQL does not mention payment_outbox:\n%s", insertSQL)
	}
}

func TestFetchPendingSQL_OrdersByCreatedAt(t *testing.T) {
	upper := strings.ToUpper(fetchPendingSQL)
	if !strings.Contains(upper, "ORDER BY CREATED_AT") {
		t.Errorf("fetchPendingSQL missing ORDER BY created_at:\n%s", fetchPendingSQL)
	}
	if !strings.Contains(fetchPendingSQL, "status = 'PENDING'") &&
		!strings.Contains(fetchPendingSQL, "status='PENDING'") {
		t.Errorf("fetchPendingSQL missing status filter:\n%s", fetchPendingSQL)
	}
	if !strings.Contains(upper, "FOR UPDATE SKIP LOCKED") {
		t.Errorf("fetchPendingSQL missing FOR UPDATE SKIP LOCKED (regression risk: two pollers would publish the same row):\n%s", fetchPendingSQL)
	}
}

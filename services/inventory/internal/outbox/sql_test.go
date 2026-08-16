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
	if !strings.Contains(insertSQL, "inventory_outbox") {
		t.Errorf("insertSQL does not mention inventory_outbox:\n%s", insertSQL)
	}
}

func TestFetchPendingSQL_OrdersByCreatedAt(t *testing.T) {
	if !strings.Contains(strings.ToLower(fetchPendingSQL), "order by created_at") {
		t.Errorf("fetchPendingSQL missing ORDER BY created_at:\n%s", fetchPendingSQL)
	}
}

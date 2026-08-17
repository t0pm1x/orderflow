package chaos_test

import (
	"net/http"
	"os"
	"testing"
	"time"
)

// waitForHealth polls url until it returns 200 or timeout elapses,
// failing the test on timeout.
func waitForHealth(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("service at %s did not become healthy within %s", url, timeout)
}

// osReadFile is a small wrapper around os.ReadFile that fails the test
// on I/O error so the chaos test bodies stay focused on assertions.
func osReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

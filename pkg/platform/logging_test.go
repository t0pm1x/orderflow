package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewLogger_LevelFromEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	logger := NewLogger()
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("expected debug level")
	}
}

func TestLogWithTrace_NoSpan(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))
	enhanced := LogWithTrace(context.Background(), base)
	enhanced.Info("test")
	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if _, has := entry["trace_id"]; has {
		t.Error("expected no trace_id without span")
	}
}

// TestNewLogger_EmitsJSON is the OBS-6 regression net. Before the
// fix, slog.Default() in the four service binaries was the Go
// stdlib text handler (time=... msg=... key=value) so downstream
// log shipping could not parse the lines as JSON. NewLogger must
// emit a JSON object per log line, written to stderr.
//
// Redirecting os.Stderr must happen BEFORE NewLogger is called so
// the JSONHandler captures the test pipe as its io.Writer (Go's
// slog.JSONHandler snapshots the writer at construction time).
func TestNewLogger_EmitsJSON(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	origStderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = origStderr })

	// Construct the logger WHILE os.Stderr points at the pipe so
	// the JSONHandler records the pipe as its writer. Restoring
	// os.Stderr before logger.Info would still write to the pipe
	// (the handler holds the writer by value), which is what we
	// want.
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	logger.Info("smoke", "k", "v")
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(bytes.TrimSpace(out)) == 0 {
		t.Fatal("expected log output, got empty bytes")
	}
	var entry map[string]any
	if jerr := json.Unmarshal(bytes.TrimSpace(out), &entry); jerr != nil {
		t.Fatalf("NewLogger output is not valid JSON: %v\noutput: %s", jerr, out)
	}
	if entry["msg"] != "smoke" {
		t.Errorf("msg field: got %v want smoke", entry["msg"])
	}
	if entry["k"] != "v" {
		t.Errorf("k field: got %v want v", entry["k"])
	}
}

// TestSetDefault_EmitsJSON is the OBS-6 integration regression net:
// after calling slog.SetDefault(NewLogger()), slog.Default() must
// emit JSON. This is the exact sequence the four service binaries
// follow at startup; if SetDefault is bypassed (e.g. a future refactor
// that passes the logger to every caller instead of relying on
// slog.Default()), /var/log lines would silently regress to text.
func TestSetDefault_EmitsJSON(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	origStderr := os.Stderr
	origDefault := slog.Default()
	os.Stderr = w
	t.Cleanup(func() {
		os.Stderr = origStderr
		slog.SetDefault(origDefault)
	})

	slog.SetDefault(NewLogger())
	slog.Default().Info("smoke", "k", "v")
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(bytes.TrimSpace(out)) == 0 {
		t.Fatal("expected log output, got empty bytes")
	}
	var entry map[string]any
	if jerr := json.Unmarshal(bytes.TrimSpace(out), &entry); jerr != nil {
		t.Fatalf("slog.Default() after SetDefault(NewLogger()) is not JSON: %v\noutput: %s", jerr, out)
	}
	if entry["msg"] != "smoke" {
		t.Errorf("msg field: got %v want smoke", entry["msg"])
	}
}

// TestRedact_SEC11 verifies the v1.2 fix for SEC-11: pre-v1.2
// returned the first-6+last-4 chars of the raw input, leaking
// scheme/host/port for URLs (e.g. "postgres://orderflow:secret@db:
// 5432/..." → "postgr…:5432"). The SHA-256 first-8-hex algorithm
// is opaque and stable: same input → same fingerprint; different
// inputs → different fingerprints.
func TestRedact_SEC11(t *testing.T) {
	cases := []struct {
		name, in, wantEmpty, wantFilled string
	}{
		{"empty", "", "<unset>", ""},
		{"short", "abc", "", "sha256:"},
		{"long url", "postgres://user:secret@host:5432/db", "", "sha256:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Redact(tc.in)
			if tc.in == "" {
				if got != "<unset>" {
					t.Errorf("empty input: got %q want <unset>", got)
				}
				return
			}
			if len(got) < len("sha256:") || got[:len("sha256:")] != "sha256:" {
				t.Errorf("non-empty input: got %q want sha256: prefix", got)
			}
			// Fingerprint must not contain any substring of the raw
			// input longer than 4 chars (audit SEC-11 acceptance).
			for _, frag := range []string{"secret", "host:5432", "user", "postgres://"} {
				if len(frag) > 4 && contains(got, frag) {
					t.Errorf("fingerprint %q leaks substring %q of input %q", got, frag, tc.in)
				}
			}
		})
	}
	// Stable: same input → same fingerprint.
	if Redact("hello") != Redact("hello") {
		t.Error("Redact must be deterministic")
	}
	// Distinct: different inputs → different fingerprints.
	if Redact("hello") == Redact("world") {
		t.Error("Redact produced the same fingerprint for distinct inputs")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestPiiHandler_MasksConfiguredKeys is the SEC-12 regression net.
// A slog.Logger built via NewRedactingLogger must replace any value
// of a configured PII key (last_four, card_number, customer_id,
// idempotency_key, password) with "[REDACTED]" while leaving other
// keys untouched. Pre-v1.2 the bare JSON handler passed these
// values through verbatim into log shipping.
func TestPiiHandler_MasksConfiguredKeys(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	masked := slog.New(piiHandler{inner: handler})
	masked.Info("payment processed",
		"last_four", "4242",
		"card_number", "4242424242424242",
		"customer_id", "cust-abc123",
		"idempotency_key", "idem-xyz",
		"order_id", "ord-1",
		"amount_cents", 12345,
	)
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	if entry["last_four"] != "[REDACTED]" {
		t.Errorf("last_four: got %v want [REDACTED]", entry["last_four"])
	}
	if entry["card_number"] != "[REDACTED]" {
		t.Errorf("card_number: got %v want [REDACTED]", entry["card_number"])
	}
	if entry["customer_id"] != "[REDACTED]" {
		t.Errorf("customer_id: got %v want [REDACTED]", entry["customer_id"])
	}
	if entry["idempotency_key"] != "[REDACTED]" {
		t.Errorf("idempotency_key: got %v want [REDACTED]", entry["idempotency_key"])
	}
	if entry["order_id"] != "ord-1" {
		t.Errorf("order_id must NOT be redacted: got %v", entry["order_id"])
	}
	if entry["amount_cents"] != float64(12345) {
		t.Errorf("amount_cents must NOT be redacted: got %v", entry["amount_cents"])
	}
}

// TestPiiHandler_WithAttrsRedacts verifies the wrapper handles the
// WithAttrs path (slog's standard group/attr composition).
func TestPiiHandler_WithAttrsRedacts(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	masked := slog.New(piiHandler{inner: handler}).With("last_four", "0000")
	masked.Info("redact me")
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if entry["last_four"] != "[REDACTED]" {
		t.Errorf("last_four (WithAttrs): got %v want [REDACTED]", entry["last_four"])
	}
}

// TestPiiHandler_RedactsKeyAttribute is the reviewer-found regression
// net. The payment idempotency middleware emits
//
//	slog.Default().Error("idempotency: handler panic", "key", key, ...)
//
// (attribute name "key", not "idempotency_key"). The piiHandler
// must mask this; otherwise the panic path leaks the idempotency key
// verbatim. Pre-fix the entry was missing, so the panic path bypassed
// redaction.
func TestPiiHandler_RedactsKeyAttribute(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	masked := slog.New(piiHandler{inner: handler})
	masked.Error("idempotency: handler panic",
		"key", "idem-xyz-very-secret",
		"panic", "boom")
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	if entry["key"] != "[REDACTED]" {
		t.Errorf("key attribute: got %v want [REDACTED] (reviewer-found regression: SEC-12 half-fix)", entry["key"])
	}
}

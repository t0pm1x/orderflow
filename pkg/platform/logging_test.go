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

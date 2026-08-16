package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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
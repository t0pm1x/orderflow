package platform

import (
	"bytes"
	"log/slog"
	"testing"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestLogWithTrace(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))
	enhanced := LogWithTrace(t.Context(), base)
	if enhanced == nil {
		t.Fatal("expected non-nil")
	}
	enhanced.Info("test")
	if buf.Len() == 0 {
		t.Error("expected log output")
	}
}

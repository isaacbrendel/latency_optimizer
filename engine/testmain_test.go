package engine

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Keep package tests offline/deterministic unless explicitly opted in.
	if os.Getenv("LIVE_TEST") != "1" {
		_ = os.Setenv("DISABLE_LIVE_FEED", "1")
	}
	os.Exit(m.Run())
}

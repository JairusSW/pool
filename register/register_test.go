package register

import (
	"testing"

	wago "github.com/wago-org/wago"
)

// Importing this package must register the pool plugin and its workers
// dependency into the engine's global plugin registry as a side effect.
func TestRegistersPoolAndWorkers(t *testing.T) {
	ext, ok := wago.NewExtension("JairusSW/pool")
	if !ok {
		t.Fatal("pool extension was not registered")
	}
	if got := ext.Info().ID; got != "wago.pool" {
		t.Fatalf("pool extension ID = %q, want wago.pool", got)
	}
	if _, ok := wago.NewExtension("wago-org/workers"); !ok {
		t.Fatal("workers dependency was not registered")
	}
}

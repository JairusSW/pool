package pool

import (
	"strings"
	"testing"

	"github.com/wago-org/wago"
	_ "github.com/wago-org/workers/register" // globally register the workers plugin
)

// TestPlanPathBindsWorkersService exercises the production wiring path: under
// LoadPlugins the runtime resolves workers.ServiceKey and binds it into the pool
// plugin automatically, with no explicit WithWorkers. A successful load proves
// the service dependency and its type match end-to-end.
func TestPlanPathBindsWorkersService(t *testing.T) {
	rt := wago.NewRuntime()
	defer rt.Close()
	err := rt.LoadPlugins([]wago.PluginConfig{
		{Name: "wago-org/workers", Capabilities: []wago.PluginCapability{
			wago.PluginManagedInstances, wago.PluginInstanceHooks,
		}},
		{Name: "wago-org/pool"},
	})
	if err != nil {
		t.Fatalf("LoadPlugins(workers, pool): %v", err)
	}
}

// TestPlanPathMissingWorkersFails confirms pool declares a hard dependency: loading
// it without a workers provider is a resolve error, not a silent half-wiring.
func TestPlanPathMissingWorkersFails(t *testing.T) {
	rt := wago.NewRuntime()
	defer rt.Close()
	err := rt.LoadPlugins([]wago.PluginConfig{{Name: "wago-org/pool"}})
	if err == nil {
		t.Fatal("LoadPlugins(pool alone) succeeded; want a missing-provider error")
	}
	if !strings.Contains(err.Error(), "wago.workers/v1") {
		t.Fatalf("error %q does not mention the required workers service", err)
	}
}

func TestEnsureWiredErrors(t *testing.T) {
	// No explicit workers and no service ref: the service is inactive.
	p := newPools(nil, Limits{})
	if err := p.ensureWired(); err != ErrPoolsInactive {
		t.Fatalf("ensureWired(no workers, no ref) = %v, want ErrPoolsInactive", err)
	}
	// Public operations surface the same error rather than panicking.
	if _, err := p.Submit(1, 0, nil); err != ErrPoolsInactive {
		t.Fatalf("Submit on inactive service = %v, want ErrPoolsInactive", err)
	}
}

func TestPoolErrorStrings(t *testing.T) {
	for _, e := range []poolError{
		ErrPoolNotFound, ErrPoolBackpressure, ErrPoolDraining, ErrInvalidPoolOptions,
		ErrPoolWorkerLimit, ErrPoolsClosed,
	} {
		if e.Error() == "" {
			t.Fatalf("empty error string for sentinel %v", string(e))
		}
	}
}

func TestSeedForHonorsExplicitSeed(t *testing.T) {
	if got := (PoolOptions{HashSeed: 42}).seedFor(7); got != 42 {
		t.Fatalf("explicit seed = %d, want 42", got)
	}
	// Derived seed is stable per pool ID and differs between IDs.
	var opts PoolOptions
	a, b := opts.seedFor(1), opts.seedFor(2)
	if a == b || a != opts.seedFor(1) {
		t.Fatalf("derived seeds not stable/distinct: %d %d", a, b)
	}
}

package pool

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wago-org/wago"
	wagoplugin "github.com/wago-org/wago/plugin"
	"github.com/wago-org/wago/tests/wasmtest"
	"github.com/wago-org/workers"
)

const (
	opNext       = 0
	opCreate     = 1
	opReconcile  = 2
	opScale      = 3
	opSubmitFrom = 4
)

type registerFunc func(*wago.Registrar) error

func (f registerFunc) Register(reg *wago.Registrar) error { return f(reg) }

func testPluginSet(t *testing.T, providers []wago.PluginProvider, configs map[string]json.RawMessage) wago.PluginSet {
	t.Helper()
	set := wago.PluginSet{Providers: providers}
	for _, provider := range providers {
		digest, err := wago.DefinitionDigest(provider.Definition)
		if err != nil {
			t.Fatal(err)
		}
		selection := wago.PluginSelection{
			ID: provider.Definition.ID, DefinitionDigest: digest, Direct: true,
			Dependencies: map[string]string{}, Config: configs[provider.Definition.ID],
		}
		for _, requirement := range provider.Definition.Requires {
			selection.Dependencies[requirement.ID] = requirement.Version
		}
		for _, authority := range provider.Definition.Authorities {
			selection.Grants = append(selection.Grants, wago.AuthorityGrant{Name: authority.Name, Scope: authority.Scope})
		}
		for _, requirement := range provider.Definition.Consumes {
			var owners []string
			for _, candidate := range providers {
				for _, provided := range candidate.Definition.Provides {
					if provided.ID == requirement.ID && provided.Major == requirement.Major {
						owners = append(owners, candidate.Definition.ID)
					}
				}
			}
			sort.Strings(owners)
			if requirement.Mode != wago.ContractMany && len(owners) > 1 {
				owners = owners[:1]
			}
			selection.Contracts = append(selection.Contracts, wago.ContractBinding{ID: requirement.ID, Major: requirement.Major, Providers: owners})
		}
		set.Selections = append(set.Selections, selection)
	}
	return set
}

type driver struct {
	pools   *Pools
	poolRef *wagoplugin.Ref[Service]
	workers *wagoplugin.Ref[workers.Service]
	opts    PoolOptions
	message workers.Subscription
	gate    sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc

	mu         sync.Mutex
	lastPool   PoolID
	allPools   []PoolID
	createErrs []error
	msgs       int
	got        map[WorkerID]int
	trapTag    uint64
	trapNext   map[WorkerID]bool
}

func driverProvider(d *driver) wago.PluginProvider {
	definition := wago.PluginDefinition{
		ID: "example.com/pool-driver", Version: "1.0.0",
		Provenance: wago.PluginProvenance{Repository: "https://example.com/pool-driver", License: "MIT"},
		Requires: []wago.PluginRequirement{
			{ID: PluginID, Version: "^0.1.0"},
			{ID: workers.PluginID, Version: "^0.1.0"},
		},
		Authorities: []wago.AuthorityRequest{{
			Name: wago.AuthorityHostImportDefine, Mode: wago.AuthorityRequired,
			Reason: "bridge the integration guest to Pool and Workers",
			Scope:  wago.AuthorityScope{Modules: []string{"env"}},
		}},
		Consumes: []wago.ContractRequirement{
			{ID: Contract.ID(), Major: Contract.Major(), Mode: wago.ContractRequired},
			{ID: workers.Contract.ID(), Major: workers.Contract.Major(), Mode: wago.ContractRequired},
		},
	}
	return wago.PluginProvider{Definition: definition, New: func() wago.Plugin {
		return registerFunc(func(reg *wago.Registrar) error {
			var err error
			d.poolRef, err = wagoplugin.Require(reg, Contract)
			if err != nil {
				return err
			}
			d.workers, err = wagoplugin.Require(reg, workers.Contract)
			if err != nil {
				return err
			}
			imports, err := reg.HostImports()
			if err != nil {
				return err
			}
			module, err := imports.Module("env")
			if err != nil {
				return err
			}
			module.Func("run", d.run).Params(wago.ValI32, wago.ValI32, wago.ValI32).Results(wago.ValI32)
			return reg.Lifecycle(wago.PluginLifecycle{
				Start: d.start,
				Stop: func(context.Context) error {
					d.cancel()
					return d.workers.With(func(service workers.Service) error { return service.Unsubscribe(d.message) })
				},
			})
		})
	}}
}

func (d *driver) start(context.Context) error {
	d.ctx, d.cancel = context.WithCancel(context.Background())
	return d.workers.With(func(service workers.Service) error {
		var err error
		d.message, err = service.ObserveMessages(func(ctx *workers.MessageContext) error {
			d.mu.Lock()
			d.got[ctx.WorkerID]++
			d.msgs++
			if d.trapTag != 0 && ctx.Tag == d.trapTag {
				d.trapNext[ctx.WorkerID] = true
			}
			d.mu.Unlock()
			return nil
		})
		return err
	})
}

func (d *driver) run(caller wago.HostModule, params, results []uint64) {
	op, a, b := uint32(params[0]), uint32(params[1]), uint32(params[2])
	switch op {
	case opNext:
		d.gate.RLock()
		d.gate.RUnlock()
		if err := d.workers.With(func(service workers.Service) error { return service.DispatchNext(d.ctx, caller) }); err != nil {
			results[0] = 1
			return
		}
		_ = d.workers.With(func(service workers.Service) error {
			wid, err := service.Current(caller)
			if err != nil {
				results[0] = 1
				return nil
			}
			d.mu.Lock()
			trap := d.trapNext[wid]
			delete(d.trapNext, wid)
			d.mu.Unlock()
			if trap {
				results[0] = 2
			}
			return nil
		})
	case opCreate:
		opts := d.opts
		opts.MinWorkers = b
		_ = d.poolRef.With(func(service Service) error {
			id, err := service.Create(caller, a, opts)
			d.mu.Lock()
			if err != nil {
				d.createErrs = append(d.createErrs, err)
			} else {
				d.lastPool = id
				d.allPools = append(d.allPools, id)
				results[0] = uint64(id)
			}
			d.mu.Unlock()
			return err
		})
	case opReconcile:
		_ = d.poolRef.With(func(service Service) error { return service.Reconcile(caller, d.pool()) })
	case opScale:
		_ = d.poolRef.With(func(service Service) error { return service.Scale(caller, d.pool(), a) })
	case opSubmitFrom:
		_ = d.poolRef.With(func(service Service) error {
			_, err := service.SubmitFrom(caller, d.pool(), uint64(a), []byte("pump"))
			return err
		})
	}
}

func (d *driver) pool() PoolID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastPool
}

func (d *driver) messages() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.msgs
}

func (d *driver) pools_() []PoolID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]PoolID(nil), d.allPools...)
}

func (d *driver) distribution() map[WorkerID]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[WorkerID]int, len(d.got))
	for id, count := range d.got {
		out[id] = count
	}
	return out
}

func buildGuest(count, scaleTarget uint32) []byte {
	typeVoid := []byte{0x60, 0x00, 0x00}
	typeRun := []byte{0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x01, 0x7f}
	imp := append(wasmtest.Name("env"), wasmtest.Name("run")...)
	imp = append(imp, 0x00)
	imp = append(imp, wasmtest.ULEB(1)...)
	funcs := wasmtest.Vec(wasmtest.ULEB(0), wasmtest.ULEB(0), wasmtest.ULEB(0), wasmtest.ULEB(0), wasmtest.ULEB(0))
	exports := wasmtest.Vec(
		wasmtest.ExportEntry("start", 0x00, 2), wasmtest.ExportEntry("reconcile", 0x00, 3),
		wasmtest.ExportEntry("scale", 0x00, 4), wasmtest.ExportEntry("pump", 0x00, 5),
	)
	elem := []byte{0x00, 0x41, 0x00, 0x0b}
	elem = append(elem, wasmtest.ULEB(1)...)
	elem = append(elem, wasmtest.ULEB(1)...)
	workerBody := []byte{
		0x03, 0x40, 0x41, 0x00, 0x41, 0x00, 0x41, 0x00, 0x10, 0x00,
		0x21, 0x00, 0x20, 0x00, 0x41, 0x02, 0x46, 0x04, 0x40, 0x00, 0x0b,
		0x20, 0x00, 0x45, 0x0d, 0x00, 0x0b, 0x0b,
	}
	code := wasmtest.Vec(
		codeWithI32Locals(1, workerBody), wasmtest.Code(cmd(opCreate, 0, count)),
		wasmtest.Code(cmd(opReconcile, 0, 0)), wasmtest.Code(cmd(opScale, scaleTarget, 0)),
		wasmtest.Code(cmd(opSubmitFrom, 7, 0)),
	)
	return wasmtest.Module(
		wasmtest.Section(1, wasmtest.Vec(typeVoid, typeRun)), wasmtest.Section(2, wasmtest.Vec(imp)),
		wasmtest.Section(3, funcs), wasmtest.Section(4, wasmtest.Vec([]byte{0x70, 0x00, 0x01})),
		wasmtest.Section(7, exports), wasmtest.Section(9, wasmtest.Vec(elem)), wasmtest.Section(10, code),
	)
}

func cmd(op, a, b uint32) []byte {
	body := []byte{0x41}
	body = append(body, wasmtest.SLEB32(int32(op))...)
	body = append(body, 0x41)
	body = append(body, wasmtest.SLEB32(int32(a))...)
	body = append(body, 0x41)
	body = append(body, wasmtest.SLEB32(int32(b))...)
	return append(body, 0x10, 0x00, 0x1a, 0x0b)
}

func codeWithI32Locals(n uint32, body []byte) []byte {
	decls := append([]byte{0x01}, wasmtest.ULEB(n)...)
	decls = append(decls, 0x7f)
	fn := append(decls, body...)
	return append(wasmtest.ULEB(uint32(len(fn))), fn...)
}

type rig struct {
	d   *driver
	rt  *wago.Runtime
	in  *wago.Instance
	mod *wago.Module
}

func newRig(t *testing.T, opts PoolOptions, count, scaleTarget uint32) *rig {
	return newRigLim(t, opts, count, scaleTarget, Limits{MaxPools: 32, MaxWorkersPerPool: 64, MaxTotalWorkers: 256, RunnableWorkers: 256})
}

func newRigLim(t *testing.T, opts PoolOptions, count, scaleTarget uint32, limits Limits) *rig {
	t.Helper()
	d := &driver{opts: opts, got: map[WorkerID]int{}, trapNext: map[WorkerID]bool{}}
	poolProvider := Provider()
	var poolPlugin *plugin
	poolProvider.New = func() wago.Plugin { poolPlugin = new(plugin); return poolPlugin }
	// Deliberately link the graph in reverse dependency order. LoadPlugins must
	// derive Workers -> Pool -> consumer from Requires and contract bindings.
	providers := []wago.PluginProvider{driverProvider(d), poolProvider, workers.Provider()}
	limits = normalizeLimits(limits)
	poolConfig, err := json.Marshal(map[string]uint32{
		"maxPools": limits.MaxPools, "maxWorkersPerPool": limits.MaxWorkersPerPool,
		"maxTotalWorkers": limits.MaxTotalWorkers, "runnableWorkers": limits.RunnableWorkers,
	})
	if err != nil {
		t.Fatal(err)
	}
	set := testPluginSet(t, providers, map[string]json.RawMessage{
		workers.PluginID: json.RawMessage(`{"maxLiveWorkers":256,"maxQueueBytes":536870912}`),
		PluginID:         poolConfig,
	})
	rt := wago.NewRuntime()
	if err := rt.LoadPlugins(context.Background(), set); err != nil {
		t.Fatal(err)
	}
	d.pools = poolPlugin.service
	mod, err := rt.Compile(buildGuest(count, scaleTarget))
	if err != nil {
		t.Fatal(err)
	}
	in, err := rt.Instantiate(nil, mod)
	if err != nil {
		t.Fatal(err)
	}
	return &rig{d: d, rt: rt, in: in, mod: mod}
}

func (r *rig) close() { _ = r.in.Close(); _ = r.rt.Close() }

func (r *rig) invoke(t *testing.T, name string) {
	t.Helper()
	if _, err := r.in.Invoke(name); err != nil {
		t.Fatal(err)
	}
}

func (r *rig) stats(t *testing.T) PoolStats {
	t.Helper()
	stats, err := r.d.pools.Stats(r.d.pool())
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestContractGraphRoutesAndScales(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin, MaxWorkers: 4}, 2, 4)
	defer r.close()
	r.invoke(t, "start")
	if got := r.stats(t).Live; got != 2 {
		t.Fatalf("live = %d, want 2", got)
	}
	for i := 0; i < 20; i++ {
		if _, err := r.d.pools.Submit(r.d.pool(), uint64(i), []byte("task")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "tasks", func() bool { return r.d.messages() == 20 })
	r.invoke(t, "scale")
	waitFor(t, "scale", func() bool { return r.stats(t).Live == 4 })
	if err := r.rt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.d.poolRef.With(func(Service) error { return nil }); !errors.Is(err, wago.ErrPermissionDenied) {
		t.Fatalf("pool contract after close = %v", err)
	}
	if err := r.d.workers.With(func(workers.Service) error { return nil }); !errors.Is(err, wago.ErrPermissionDenied) {
		t.Fatalf("driver Workers contract after close = %v", err)
	}
	if err := r.d.pools.ref.With(func(workers.Service) error { return nil }); !errors.Is(err, wago.ErrPermissionDenied) {
		t.Fatalf("Pool's Workers contract after close = %v", err)
	}
	if pools := r.d.pools.List(); len(pools) != 0 {
		t.Fatalf("Pool retained resources after close: %v", pools)
	}
}

func TestProviderRejectsMissingWorkersAndStrictConfig(t *testing.T) {
	if err := wago.ValidatePluginSet(testPluginSet(t, []wago.PluginProvider{Provider()}, nil)); err == nil {
		t.Fatal("pool without workers succeeded")
	}
	for _, config := range []json.RawMessage{
		json.RawMessage(`null`),
		json.RawMessage(`[]`),
		json.RawMessage(`{"maxPools":null}`),
		json.RawMessage(`{"maxPools":1,"maxPools":2}`),
		json.RawMessage(`{"unknown":1}`),
		json.RawMessage(`{"maxPools":0}`),
		json.RawMessage(`{"maxWorkersPerPool":10,"maxTotalWorkers":2}`),
	} {
		set := testPluginSet(t, []wago.PluginProvider{workers.Provider(), Provider()}, map[string]json.RawMessage{PluginID: config})
		if err := wago.ValidatePluginSet(set); err == nil {
			t.Fatalf("accepted invalid config %s", config)
		}
	}
}

package pool

import (
	"sync"
	"testing"
	"time"

	"github.com/wago-org/wago"
	"github.com/wago-org/wago/testutil/wasmtest"
	"github.com/wago-org/workers"
)

// Guest ABI op codes for the single `env.run` host import used by the test guest.
const (
	opNext       = 0 // worker: drain one message; -> 0 continue, 1 stop, 2 trap
	opCreate     = 1 // main: create a pool on table a with b workers; -> pool id
	opReconcile  = 2 // main: reconcile the last-created pool
	opScale      = 3 // main: scale the last-created pool to a
	opSubmitFrom = 4 // main: reconcile-then-route a task with tag a
)

// driver is a tiny host plugin that bridges the guest to the Pools and Workers
// services. It is the "embedder" layer: it owns the host imports and passes the
// live caller through to pool operations, exactly as a real application would.
type driver struct {
	once       sync.Once
	pools      *Pools
	workers    *workers.Workers
	createOpts PoolOptions

	gate sync.RWMutex // write-locked to pause workers before they dispatch

	mu         sync.Mutex
	lastPool   PoolID
	allPools   []PoolID
	createErrs []error
	msgs       int
	got        map[WorkerID]int
	trapTag    uint64
	trapNext   map[WorkerID]bool
}

func newDriver(pools *Pools, wk *workers.Workers, opts PoolOptions) *driver {
	return &driver{pools: pools, workers: wk, createOpts: opts, got: map[WorkerID]int{}, trapNext: map[WorkerID]bool{}}
}

func (d *driver) Info() wago.ExtensionInfo {
	return wago.ExtensionInfo{
		ID: "pool.driver", Name: "pool test driver",
		RequiresCapabilities: []wago.PluginCapability{wago.PluginHostImports},
	}
}

func (d *driver) Register(reg *wago.Registry) error {
	imports, err := reg.HostImports()
	if err != nil {
		return err
	}
	imports.Module("env").Func("run", d.run).
		Params(wago.ValI32, wago.ValI32, wago.ValI32).Results(wago.ValI32)
	return nil
}

// wire registers the message-counting observer once, before any worker is
// spawned (the first run call is the guest's create).
func (d *driver) wire() {
	d.once.Do(func() {
		d.workers.OnMessage(func(ctx *workers.MessageContext) error {
			d.mu.Lock()
			d.got[ctx.WorkerID]++
			d.msgs++
			if d.trapTag != 0 && ctx.Tag == d.trapTag {
				d.trapNext[ctx.WorkerID] = true
			}
			d.mu.Unlock()
			return nil
		})
	})
}

func (d *driver) run(caller wago.HostModule, params, results []uint64) {
	d.wire()
	op, a, b := uint32(params[0]), uint32(params[1]), uint32(params[2])
	switch op {
	case opNext:
		// Gate before dispatching so a paused pool holds a real backlog.
		d.gate.RLock()
		d.gate.RUnlock()
		if err := d.workers.DispatchNext(caller); err != nil {
			results[0] = 1
			return
		}
		if wid, err := d.workers.Current(caller); err == nil {
			d.mu.Lock()
			trap := d.trapNext[wid]
			delete(d.trapNext, wid)
			d.mu.Unlock()
			if trap {
				results[0] = 2
				return
			}
		}
		results[0] = 0
	case opCreate:
		opts := d.createOpts
		opts.MinWorkers = b
		id, err := d.pools.Create(caller, a, opts)
		d.mu.Lock()
		if err == nil {
			d.lastPool = id
			d.allPools = append(d.allPools, id)
		} else {
			d.createErrs = append(d.createErrs, err)
		}
		d.mu.Unlock()
		if err == nil {
			results[0] = uint64(id)
		}
	case opReconcile:
		_ = d.pools.Reconcile(caller, d.pool())
	case opScale:
		_ = d.pools.Scale(caller, d.pool(), a)
	case opSubmitFrom:
		_, _ = d.pools.SubmitFrom(caller, d.pool(), uint64(a), []byte("pump"))
	}
}

func (d *driver) pool() PoolID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastPool
}

func (d *driver) pools_() []PoolID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]PoolID(nil), d.allPools...)
}

func (d *driver) messages() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.msgs
}

func (d *driver) distribution() map[WorkerID]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[WorkerID]int{}
	for k, v := range d.got {
		out[k] = v
	}
	return out
}

// buildGuest hand-assembles a wasm module exposing:
//   - table[0] = $worker: loop { r=run(NEXT); if r==2 trap; if r!=0 return }
//   - export "start":     run(CREATE, 0, count)
//   - export "reconcile": run(RECONCILE, 0, 0)
//   - export "scale":     run(SCALE, scaleTarget, 0)
func buildGuest(count, scaleTarget uint32) []byte {
	// Types: 0 = ()->(), 1 = (i32,i32,i32)->(i32).
	typeVoid := []byte{0x60, 0x00, 0x00}
	typeRun := []byte{0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x01, 0x7f}
	types := wasmtest.Vec(typeVoid, typeRun)

	// Import env.run : type 1.
	imp := append(wasmtest.Name("env"), wasmtest.Name("run")...)
	imp = append(imp, 0x00)                // func import
	imp = append(imp, wasmtest.ULEB(1)...) // type index 1
	imports := wasmtest.Vec(imp)

	// Five local funcs, all type 0. (Imported run is func index 0.)
	funcs := wasmtest.Vec(wasmtest.ULEB(0), wasmtest.ULEB(0), wasmtest.ULEB(0), wasmtest.ULEB(0), wasmtest.ULEB(0))

	// One funcref table, min size 1.
	table := wasmtest.Vec([]byte{0x70, 0x00, 0x01})

	// Exports: start=func2, reconcile=func3, scale=func4, pump=func5.
	exports := wasmtest.Vec(
		wasmtest.ExportEntry("start", 0x00, 2),
		wasmtest.ExportEntry("reconcile", 0x00, 3),
		wasmtest.ExportEntry("scale", 0x00, 4),
		wasmtest.ExportEntry("pump", 0x00, 5),
	)

	// Active element segment: table[0] = func1 ($worker).
	elem := []byte{0x00, 0x41, 0x00, 0x0b}
	elem = append(elem, wasmtest.ULEB(1)...) // one entry
	elem = append(elem, wasmtest.ULEB(1)...) // func index 1
	elems := wasmtest.Vec(elem)

	// $worker with one i32 local: loop { r=run(0,0,0); if r==2 unreachable; if r==0 continue }.
	workerBody := []byte{
		0x03, 0x40, // loop void
		0x41, 0x00, 0x41, 0x00, 0x41, 0x00, // i32.const 0 x3 (op,a,b)
		0x10, 0x00, // call 0 (run)
		0x21, 0x00, // local.set 0
		0x20, 0x00, // local.get 0
		0x41, 0x02, // i32.const 2
		0x46,       // i32.eq
		0x04, 0x40, // if void
		0x00,       // unreachable
		0x0b,       // end if
		0x20, 0x00, // local.get 0
		0x45,       // i32.eqz
		0x0d, 0x00, // br_if 0 (continue loop while r==0)
		0x0b, // end loop
		0x0b, // end func
	}
	worker := codeWithI32Locals(1, workerBody)

	start := wasmtest.Code(cmd(opCreate, 0, count))
	reconcile := wasmtest.Code(cmd(opReconcile, 0, 0))
	scale := wasmtest.Code(cmd(opScale, scaleTarget, 0))
	pump := wasmtest.Code(cmd(opSubmitFrom, 7, 0))
	code := wasmtest.Vec(worker, start, reconcile, scale, pump)

	return wasmtest.Module(
		wasmtest.Section(1, types),
		wasmtest.Section(2, imports),
		wasmtest.Section(3, funcs),
		wasmtest.Section(4, table),
		wasmtest.Section(7, exports),
		wasmtest.Section(9, elems),
		wasmtest.Section(10, code),
	)
}

// cmd emits `run(op, a, b); drop; end` for a void command function.
func cmd(op, a, b uint32) []byte {
	body := []byte{0x41}
	body = append(body, wasmtest.SLEB32(int32(op))...)
	body = append(body, 0x41)
	body = append(body, wasmtest.SLEB32(int32(a))...)
	body = append(body, 0x41)
	body = append(body, wasmtest.SLEB32(int32(b))...)
	body = append(body, 0x10, 0x00, 0x1a, 0x0b) // call 0; drop; end
	return body
}

// codeWithI32Locals wraps a function body that declares n i32 locals into a code
// section entry (wasmtest.Code only supports zero locals).
func codeWithI32Locals(n uint32, body []byte) []byte {
	var decls []byte
	if n == 0 {
		decls = []byte{0x00}
	} else {
		decls = append([]byte{0x01}, wasmtest.ULEB(n)...)
		decls = append(decls, 0x7f) // i32
	}
	fn := append(decls, body...)
	return append(wasmtest.ULEB(uint32(len(fn))), fn...)
}

// rig wires a runtime with the workers plugin (generous limits), the pool
// plugin, and the test driver, then compiles and instantiates the guest.
type rig struct {
	d  *driver
	rt *wago.Runtime
	in *wago.Instance
}

func newRig(t *testing.T, opts PoolOptions, count, scaleTarget uint32) *rig {
	return newRigLim(t, opts, count, scaleTarget, Limits{MaxPools: 32, MaxWorkersPerPool: 64, MaxTotalWorkers: 256})
}

func newRigLim(t *testing.T, opts PoolOptions, count, scaleTarget uint32, limits Limits) *rig {
	t.Helper()
	rt := wago.NewRuntime()
	wk := workers.New(workers.WithLimits(workers.WorkerLimits{MaxLiveWorkers: 256, MaxQueueBytes: 512 << 20}))
	if err := rt.Use(wk); err != nil {
		t.Fatalf("use workers: %v", err)
	}
	poolPlugin := New(WithWorkers(wk.Service()), WithLimits(limits))
	if err := rt.Use(poolPlugin); err != nil {
		t.Fatalf("use pool: %v", err)
	}
	d := newDriver(poolPlugin.Service(), wk.Service(), opts)
	if err := rt.Use(d); err != nil {
		t.Fatalf("use driver: %v", err)
	}
	mod, err := rt.Compile(buildGuest(count, scaleTarget))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	in, err := rt.Instantiate(nil, mod)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	return &rig{d: d, rt: rt, in: in}
}

func (r *rig) invoke(t *testing.T, name string) {
	t.Helper()
	if _, err := r.in.Invoke(name); err != nil {
		t.Fatalf("invoke %s: %v", name, err)
	}
}

func (r *rig) close() {
	_ = r.in.Close()
	_ = r.rt.Close()
}

func (r *rig) stats(t *testing.T) PoolStats {
	t.Helper()
	st, err := r.d.pools.Stats(r.d.pool())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return st
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

func TestIntegrationRoundRobinLoadBalancing(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin}, 4, 0)
	defer r.close()
	r.invoke(t, "start")

	st := r.stats(t)
	if st.Live != 4 {
		t.Fatalf("live = %d, want 4", st.Live)
	}

	const n = 200
	for i := 0; i < n; i++ {
		if _, err := r.d.pools.Submit(r.d.pool(), uint64(i), []byte("task")); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	waitFor(t, "all tasks dispatched", func() bool { return r.d.messages() == n })

	st = r.stats(t)
	if st.Routed != n || st.Rejected != 0 {
		t.Fatalf("routed=%d rejected=%d, want %d,0", st.Routed, st.Rejected, n)
	}
	// Round-robin over 4 workers => exactly n/4 each.
	dist := r.d.distribution()
	if len(dist) != 4 {
		t.Fatalf("distribution touched %d workers, want 4: %v", len(dist), dist)
	}
	for wid, c := range dist {
		if c != n/4 {
			t.Fatalf("worker %d got %d, want %d (even round-robin)", wid, c, n/4)
		}
	}
}

func TestIntegrationBroadcast(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: Broadcast}, 5, 0)
	defer r.close()
	r.invoke(t, "start")

	delivered, err := r.d.pools.Broadcast(r.d.pool(), 7, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	if delivered != 5 {
		t.Fatalf("broadcast delivered to %d, want 5", delivered)
	}
	waitFor(t, "every worker received the broadcast", func() bool { return r.d.messages() == 5 })
	dist := r.d.distribution()
	if len(dist) != 5 {
		t.Fatalf("broadcast reached %d workers, want 5", len(dist))
	}
}

func TestIntegrationRestartOnFailure(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Restart: RestartOnFailure}, 4, 0)
	defer r.close()

	r.d.mu.Lock()
	r.d.trapTag = 0xDEAD
	r.d.mu.Unlock()
	r.invoke(t, "start")

	// Send one poisoned task; whichever worker gets it traps and fails.
	if _, err := r.d.pools.Submit(r.d.pool(), 0xDEAD, []byte("boom")); err != nil {
		t.Fatalf("submit trap: %v", err)
	}
	waitFor(t, "a worker to fail", func() bool { return r.stats(t).Failed >= 1 })
	if got := r.stats(t).Live; got != 3 {
		t.Fatalf("after failure live = %d, want 3", got)
	}

	// Reconcile from a live caller heals the pool back to full strength.
	r.invoke(t, "reconcile")
	waitFor(t, "pool to heal", func() bool { return r.stats(t).Live == 4 })
	st := r.stats(t)
	if st.Failed != 1 || st.Restarted < 1 {
		t.Fatalf("after heal failed=%d restarted=%d, want 1,>=1", st.Failed, st.Restarted)
	}
}

func TestIntegrationScaleGrows(t *testing.T) {
	// Manual pool with headroom (band [2,6], autoscale off): Scale is authoritative
	// and grows the pool to the requested size from a live caller.
	r := newRig(t, PoolOptions{Strategy: RoundRobin, MaxWorkers: 6}, 2, 6)
	defer r.close()
	r.invoke(t, "start")
	if got := r.stats(t).Live; got != 2 {
		t.Fatalf("initial live = %d, want 2", got)
	}
	r.invoke(t, "scale")
	waitFor(t, "pool to scale up", func() bool { return r.stats(t).Live == 6 })

	// A second reconcile must not disturb a manual pool (autoscale off).
	r.invoke(t, "reconcile")
	if got := r.stats(t).Live; got != 6 {
		t.Fatalf("manual pool drifted after reconcile: live = %d, want 6", got)
	}
}

func TestIntegrationScaleClamped(t *testing.T) {
	// Fixed pool (Max==Min==3): Scale beyond capacity is clamped, not grown.
	r := newRig(t, PoolOptions{Strategy: RoundRobin}, 3, 10)
	defer r.close()
	r.invoke(t, "start")
	r.invoke(t, "scale")
	if got := r.stats(t).Live; got != 3 {
		t.Fatalf("fixed-pool scale clamped live = %d, want 3", got)
	}
}

func TestIntegrationAutoscaleFromBacklog(t *testing.T) {
	// Elastic band [1,6], target 4 tasks/worker. Pause workers, build a backlog of
	// 12 on the single worker, then reconcile: desired = ceil(12/4) = 3.
	r := newRig(t, PoolOptions{Strategy: RoundRobin, MaxWorkers: 6, TargetPerWorker: 4}, 1, 0)
	defer r.close()

	r.d.gate.Lock() // pause: workers block before dispatching
	resumed := false
	resume := func() {
		if !resumed {
			r.d.gate.Unlock()
			resumed = true
		}
	}
	defer resume() // never close the runtime while workers are gated

	r.invoke(t, "start")
	if got := r.stats(t).Live; got != 1 {
		t.Fatalf("initial live = %d, want 1", got)
	}

	const backlog = 12
	for i := 0; i < backlog; i++ {
		if _, err := r.d.pools.Submit(r.d.pool(), uint64(i), []byte("q")); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	waitFor(t, "backlog to register", func() bool { return r.stats(t).Outstanding == backlog })

	r.invoke(t, "reconcile")
	if got := r.stats(t).Live; got != 3 {
		t.Fatalf("autoscaled live = %d, want 3 (ceil(12/4))", got)
	}

	resume()
	waitFor(t, "backlog to drain", func() bool { return r.stats(t).Outstanding == 0 })
}

func TestIntegrationDrainGraceful(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin}, 4, 0)
	defer r.close()
	r.invoke(t, "start")
	waitFor(t, "workers to idle", func() bool { return r.stats(t).Live == 4 })

	if err := r.d.pools.Drain(r.d.pool()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	waitFor(t, "pool to drain to zero", func() bool { return r.stats(t).Live == 0 })
	if _, err := r.d.pools.Submit(r.d.pool(), 1, nil); err != ErrPoolDraining {
		t.Fatalf("submit after drain = %v, want ErrPoolDraining", err)
	}
}

func TestIntegrationDestroy(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin}, 4, 0)
	defer r.close()
	r.invoke(t, "start")
	id := r.d.pool()
	waitFor(t, "workers to idle", func() bool {
		st, _ := r.d.pools.Stats(id)
		return st.Live == 4
	})
	if err := r.d.pools.Destroy(id); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := r.d.pools.Stats(id); err != ErrPoolNotFound {
		t.Fatalf("stats after destroy = %v, want ErrPoolNotFound", err)
	}
	if len(r.d.pools.List()) != 0 {
		t.Fatalf("pools remain after destroy: %v", r.d.pools.List())
	}
}

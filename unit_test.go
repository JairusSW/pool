package pool

import (
	"runtime"
	"testing"

	"github.com/wago-org/workers"
)

func TestOptimalWorkers(t *testing.T) {
	if got := OptimalWorkers(); got < 1 {
		t.Fatalf("OptimalWorkers() = %d, want >= 1", got)
	}
	if got, want := OptimalWorkers(), uint32(runtime.GOMAXPROCS(0)); got != want {
		t.Fatalf("OptimalWorkers() = %d, want GOMAXPROCS %d", got, want)
	}
}

func TestAutoMaxWorkersFromCPU(t *testing.T) {
	prev := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prev)

	// Elastic pool with no explicit ceiling: MaxWorkers = OptimalWorkers().
	o, err := normalizePoolOptions(PoolOptions{TargetPerWorker: 8})
	if err != nil {
		t.Fatal(err)
	}
	if o.MaxWorkers != 4 {
		t.Fatalf("auto MaxWorkers = %d, want 4 (GOMAXPROCS)", o.MaxWorkers)
	}
	// A MinWorkers above the optimal raises the ceiling to MinWorkers.
	if o, _ = normalizePoolOptions(PoolOptions{TargetPerWorker: 8, MinWorkers: 6}); o.MaxWorkers != 6 {
		t.Fatalf("auto MaxWorkers = %d, want 6 (>= MinWorkers)", o.MaxWorkers)
	}
	// No autoscaling: MaxWorkers stays at MinWorkers; the CPU is not consulted.
	if o, _ = normalizePoolOptions(PoolOptions{MinWorkers: 2}); o.MaxWorkers != 2 {
		t.Fatalf("fixed MaxWorkers = %d, want 2", o.MaxWorkers)
	}
	// An explicit ceiling is always respected.
	if o, _ = normalizePoolOptions(PoolOptions{TargetPerWorker: 8, MaxWorkers: 32}); o.MaxWorkers != 32 {
		t.Fatalf("explicit MaxWorkers = %d, want 32", o.MaxWorkers)
	}
}

func TestRunnableBudgetClamp(t *testing.T) {
	p := &Pools{limits: normalizeLimits(Limits{RunnableWorkers: 4})}
	mk := func(live int, min uint32) *pool {
		pl := &pool{capMin: min, capMax: 16, perWorker: 1, desired: 8, byWorker: map[WorkerID]*member{}}
		for i := 0; i < live; i++ {
			pl.members = append(pl.members, &member{id: WorkerID(i + 1)})
		}
		return pl
	}
	// No other workers: clamp the load target (8) down to the budget (4).
	pl := mk(2, 1)
	p.total = 2
	p.clampToRunnableBudgetLocked(pl)
	if pl.desired != 4 {
		t.Fatalf("desired = %d, want 4 (budget)", pl.desired)
	}
	// Other pools hold 3 of the 4-worker budget: only one slot remains.
	pl = mk(2, 1)
	p.total = 5 // 2 here + 3 elsewhere
	p.clampToRunnableBudgetLocked(pl)
	if pl.desired != 1 {
		t.Fatalf("desired = %d, want 1 (budget minus others)", pl.desired)
	}
	// Budget already oversubscribed by others: the MinWorkers floor is preserved.
	pl = mk(1, 2)
	p.total = 10
	p.clampToRunnableBudgetLocked(pl)
	if pl.desired != 2 {
		t.Fatalf("desired = %d, want 2 (MinWorkers floor preserved)", pl.desired)
	}
	// A fixed pool (autoscale off) is never clamped.
	fixed := &pool{capMin: 3, capMax: 3, desired: 3, byWorker: map[WorkerID]*member{}}
	p.total = 100
	p.clampToRunnableBudgetLocked(fixed)
	if fixed.desired != 3 {
		t.Fatalf("fixed desired = %d, want 3 (never clamped)", fixed.desired)
	}
}

func TestNormalizeLimits(t *testing.T) {
	got := normalizeLimits(Limits{})
	if got.MaxPools != DefaultMaxPools || got.MaxWorkersPerPool != DefaultMaxWorkersPerPool || got.MaxTotalWorkers != DefaultMaxTotalWorkers {
		t.Fatalf("normalizeLimits(zero) = %+v", got)
	}
	got = normalizeLimits(Limits{MaxPools: 3})
	if got.MaxPools != 3 || got.MaxWorkersPerPool != DefaultMaxWorkersPerPool {
		t.Fatalf("partial = %+v", got)
	}
}

func TestNormalizePoolOptions(t *testing.T) {
	o, err := normalizePoolOptions(PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Strategy != RoundRobin || o.Overflow != OverflowSpill || o.Restart != RestartOnFailure {
		t.Fatalf("defaults = %+v", o)
	}
	if o.MinWorkers != 1 || o.MaxWorkers != 1 || o.MaxRestarts != DefaultMaxRestarts {
		t.Fatalf("size defaults = %+v", o)
	}

	// MaxWorkers defaults up to MinWorkers.
	o, _ = normalizePoolOptions(PoolOptions{MinWorkers: 4})
	if o.MaxWorkers != 4 {
		t.Fatalf("MaxWorkers = %d, want 4", o.MaxWorkers)
	}

	// Invalid combinations are rejected.
	for _, bad := range []PoolOptions{
		{MinWorkers: 4, MaxWorkers: 2},
		{Strategy: 99},
		{Overflow: 99},
		{Restart: 99},
	} {
		if _, err := normalizePoolOptions(bad); err != ErrInvalidPoolOptions {
			t.Fatalf("normalizePoolOptions(%+v) err = %v, want ErrInvalidPoolOptions", bad, err)
		}
	}
}

// mkPool builds an in-memory pool with n members for white-box strategy tests.
func mkPool(strategy Strategy, n int) *pool {
	pl := &pool{strategy: strategy, byWorker: map[WorkerID]*member{}, rng: 0x1234abcd}
	if strategy == ConsistentHash {
		pl.ring = newRing(777, 16)
	}
	for i := 0; i < n; i++ {
		m := &member{id: WorkerID(i + 1)}
		pl.members = append(pl.members, m)
		pl.byWorker[m.id] = m
		if pl.ring != nil {
			pl.ring.add(m.id)
		}
	}
	return pl
}

func setLoad(pl *pool, id WorkerID, outstanding uint64) {
	m := pl.byWorker[id]
	m.enqueued = outstanding
	m.dispatched = 0
}

func TestOrderRoundRobin(t *testing.T) {
	pl := mkPool(RoundRobin, 4)
	// The first choice should advance by one each call, wrapping around.
	var firsts []WorkerID
	for i := 0; i < 6; i++ {
		firsts = append(firsts, pl.order(0)[0].id)
	}
	want := []WorkerID{1, 2, 3, 4, 1, 2}
	for i := range want {
		if firsts[i] != want[i] {
			t.Fatalf("round-robin firsts = %v, want %v", firsts, want)
		}
	}
	// Every order is a full permutation of members.
	ord := pl.order(0)
	if len(ord) != 4 {
		t.Fatalf("order len = %d", len(ord))
	}
}

func TestOrderLeastLoaded(t *testing.T) {
	pl := mkPool(LeastLoaded, 4)
	setLoad(pl, 1, 10)
	setLoad(pl, 2, 3)
	setLoad(pl, 3, 7)
	setLoad(pl, 4, 0)
	ord := pl.order(0)
	if ord[0].id != 4 || ord[1].id != 2 {
		t.Fatalf("least-loaded order = %d,%d,... want 4,2,...", ord[0].id, ord[1].id)
	}
	// Fully sorted ascending by load.
	for i := 1; i < len(ord); i++ {
		if ord[i-1].outstanding() > ord[i].outstanding() {
			t.Fatalf("not sorted by load: %v", loads(ord))
		}
	}
}

func TestOrderPowerOfTwo(t *testing.T) {
	pl := mkPool(PowerOfTwo, 5)
	setLoad(pl, 1, 100)
	setLoad(pl, 2, 100)
	setLoad(pl, 3, 100)
	setLoad(pl, 4, 100)
	setLoad(pl, 5, 0) // the clear winner
	// Across many draws, the least-loaded worker should be chosen primary far more
	// often than uniform (1/5), because whenever it is sampled it wins.
	winner := 0
	const draws = 2000
	for i := 0; i < draws; i++ {
		if pl.order(0)[0].id == 5 {
			winner++
		}
	}
	// Probability worker 5 is among two independent samples ~ 1-(4/5)^2 = 0.36.
	if winner < draws/4 {
		t.Fatalf("power-of-two picked the idle worker %d/%d times; expected >~36%%", winner, draws)
	}
}

func TestOrderConsistentHashPrimaryFromRing(t *testing.T) {
	pl := mkPool(ConsistentHash, 4)
	for k := uint64(0); k < 200; k++ {
		ringID, _ := pl.ring.get(k)
		if got := pl.order(k)[0].id; got != ringID {
			t.Fatalf("key %d: order primary %d != ring %d", k, got, ringID)
		}
	}
}

func loads(ms []*member) []uint32 {
	out := make([]uint32, len(ms))
	for i, m := range ms {
		out[i] = m.outstanding()
	}
	return out
}

func TestRecomputeDesiredAutoscale(t *testing.T) {
	pl := &pool{capMin: 2, capMax: 10, perWorker: 4, byWorker: map[WorkerID]*member{}}
	add := func(id WorkerID, out uint64) {
		m := &member{id: id, enqueued: out}
		pl.members = append(pl.members, m)
		pl.byWorker[id] = m
	}
	// No load: floor at MinWorkers.
	pl.recomputeDesired()
	if pl.desired != 2 {
		t.Fatalf("idle desired = %d, want 2 (min)", pl.desired)
	}
	// 20 outstanding / 4 per worker = 5.
	for i := 0; i < 2; i++ {
		add(WorkerID(i+1), 10)
	}
	pl.recomputeDesired()
	if pl.desired != 5 {
		t.Fatalf("desired = %d, want 5", pl.desired)
	}
	// Huge load clamps at MaxWorkers.
	add(3, 1000)
	pl.recomputeDesired()
	if pl.desired != 10 {
		t.Fatalf("desired = %d, want 10 (max)", pl.desired)
	}
}

func TestRecomputeDesiredFixedPoolUnchanged(t *testing.T) {
	// perWorker==0 => fixed pool: desired is not touched by recompute.
	pl := &pool{capMin: 3, capMax: 3, desired: 3, byWorker: map[WorkerID]*member{}}
	pl.recomputeDesired()
	if pl.desired != 3 {
		t.Fatalf("fixed desired = %d, want 3", pl.desired)
	}
}

func TestSupervisionFixedPool(t *testing.T) {
	newFixed := func(policy RestartPolicy) *pool {
		return &pool{restart: policy, capMin: 3, capMax: 3, desired: 3, maxRestart: UnlimitedRestarts, byWorker: map[WorkerID]*member{}}
	}
	p := &Pools{}

	// RestartOnFailure: failures keep desired (refill), clean returns lower it.
	pl := newFixed(RestartOnFailure)
	p.applySupervisionLocked(pl, workers.WorkerFailed)
	if pl.desired != 3 || pl.restarts != 1 {
		t.Fatalf("on-failure/failed: desired=%d restarts=%d, want 3,1", pl.desired, pl.restarts)
	}
	p.applySupervisionLocked(pl, workers.WorkerReturned)
	if pl.desired != 2 {
		t.Fatalf("on-failure/returned: desired=%d, want 2", pl.desired)
	}

	// RestartAlways: every exit keeps desired and counts a restart.
	pl = newFixed(RestartAlways)
	p.applySupervisionLocked(pl, workers.WorkerReturned)
	if pl.desired != 3 || pl.restarts != 1 {
		t.Fatalf("always/returned: desired=%d restarts=%d, want 3,1", pl.desired, pl.restarts)
	}

	// RestartNever: every exit lowers desired, no restart.
	pl = newFixed(RestartNever)
	p.applySupervisionLocked(pl, workers.WorkerFailed)
	if pl.desired != 2 || pl.restarts != 0 {
		t.Fatalf("never/failed: desired=%d restarts=%d, want 2,0", pl.desired, pl.restarts)
	}
}

func TestSupervisionCrashLoopHalt(t *testing.T) {
	p := &Pools{}
	pl := &pool{restart: RestartOnFailure, capMin: 2, capMax: 2, desired: 2, maxRestart: 3, byWorker: map[WorkerID]*member{}}
	// Three failures are replaced; the fourth trips the halt and lowers desired.
	for i := 0; i < 3; i++ {
		p.applySupervisionLocked(pl, workers.WorkerFailed)
	}
	if pl.halted || pl.restarts != 3 {
		t.Fatalf("after 3 fails: halted=%v restarts=%d, want false,3", pl.halted, pl.restarts)
	}
	p.applySupervisionLocked(pl, workers.WorkerFailed)
	if !pl.halted {
		t.Fatal("4th failure did not halt replacement")
	}
	if pl.desired != 1 {
		t.Fatalf("halt desired = %d, want 1", pl.desired)
	}
}

func TestSupervisionElasticCrashLoop(t *testing.T) {
	p := &Pools{}
	pl := &pool{restart: RestartOnFailure, capMin: 1, capMax: 8, perWorker: 4, desired: 4, maxRestart: 5, byWorker: map[WorkerID]*member{}}
	for i := 0; i < 4; i++ {
		p.applySupervisionLocked(pl, workers.WorkerFailed)
		if pl.halted {
			t.Fatalf("halted early after %d failures", i+1)
		}
	}
	p.applySupervisionLocked(pl, workers.WorkerFailed) // 5th -> halt
	if !pl.halted || pl.restarts != 5 {
		t.Fatalf("elastic halt: halted=%v restarts=%d, want true,5", pl.halted, pl.restarts)
	}
	// Once halted, recompute pins desired to live rather than the load target.
	pl.members = append(pl.members, &member{id: 1})
	pl.byWorker[1] = pl.members[0]
	pl.recomputeDesired()
	if pl.desired != 1 {
		t.Fatalf("halted recompute desired = %d, want 1 (live)", pl.desired)
	}
}

func TestSupervisionNoRefillWhileDraining(t *testing.T) {
	p := &Pools{}
	pl := &pool{restart: RestartAlways, draining: true, desired: 0, byWorker: map[WorkerID]*member{}}
	p.applySupervisionLocked(pl, workers.WorkerFailed)
	if pl.desired != 0 || pl.restarts != 0 {
		t.Fatalf("draining supervision: desired=%d restarts=%d, want 0,0", pl.desired, pl.restarts)
	}
}

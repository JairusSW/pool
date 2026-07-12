package pool

import (
	"runtime"
	"sync"
	"testing"
)

func TestIntegrationAutoCeilingBoundsGrowth(t *testing.T) {
	// Pin parallelism so the CPU-derived ceiling is deterministic on any host.
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)

	// Elastic pool, no explicit MaxWorkers -> ceiling auto-sized to GOMAXPROCS (2).
	r := newRig(t, PoolOptions{Strategy: RoundRobin, MinWorkers: 1, TargetPerWorker: 1, Worker: bigMailbox}, 1, 0)
	defer r.close()

	r.d.gate.Lock() // pause workers so a backlog accumulates
	resumed := false
	resume := func() {
		if !resumed {
			r.d.gate.Unlock()
			resumed = true
		}
	}
	defer resume()

	r.invoke(t, "start")
	if got := r.stats(t).Max; got != 2 {
		t.Fatalf("auto ceiling Max = %d, want 2 (GOMAXPROCS)", got)
	}

	// A backlog large enough to want 12 workers (12 tasks / target 1)...
	const backlog = 12
	for i := 0; i < backlog; i++ {
		if _, err := r.d.pools.Submit(r.d.pool(), uint64(i), []byte("q")); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	waitFor(t, "backlog to register", func() bool { return r.stats(t).Outstanding == backlog })

	r.invoke(t, "reconcile")
	// ...is capped at the optimal count, so the pool never oversubscribes the CPU.
	if got := r.stats(t).Live; got != 2 {
		t.Fatalf("autoscaled live = %d, want 2 (optimal ceiling)", got)
	}

	resume()
	waitFor(t, "backlog to drain", func() bool { return r.stats(t).Outstanding == 0 })
}

// tinyQueue makes each worker hold exactly one message, so a paused pool
// overflows on the second submit to the same worker.
var tinyQueue = WorkerOptions{QueueCapacity: 1, MaxPayloadBytes: 16, MaxQueueBytes: 16}

func TestIntegrationOverflowReject(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Overflow: OverflowReject, Worker: tinyQueue}, 1, 0)
	defer r.close()
	r.d.gate.Lock()
	defer r.d.gate.Unlock()
	r.invoke(t, "start")
	id := r.d.pool()

	if _, err := r.d.pools.Submit(id, 1, []byte("a")); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	// Worker is paused with a full (cap-1) mailbox; the next submit is rejected.
	if _, err := r.d.pools.Submit(id, 2, []byte("b")); err != ErrPoolBackpressure {
		t.Fatalf("overflow submit = %v, want ErrPoolBackpressure", err)
	}
	if st := r.stats(t); st.Rejected != 1 || st.Routed != 1 {
		t.Fatalf("reject stats: rejected=%d routed=%d, want 1,1", st.Rejected, st.Routed)
	}
}

func TestIntegrationOverflowShed(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Overflow: OverflowShed, Worker: tinyQueue}, 1, 0)
	defer r.close()
	r.d.gate.Lock()
	defer r.d.gate.Unlock()
	r.invoke(t, "start")
	id := r.d.pool()

	if _, err := r.d.pools.Submit(id, 1, []byte("a")); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	// Shed drops the overflowing task silently and reports success.
	wid, err := r.d.pools.Submit(id, 2, []byte("b"))
	if err != nil || wid != 0 {
		t.Fatalf("shed submit = (%d,%v), want (0,nil)", wid, err)
	}
	if st := r.stats(t); st.Shed != 1 || st.Routed != 1 {
		t.Fatalf("shed stats: shed=%d routed=%d, want 1,1", st.Shed, st.Routed)
	}
}

func TestIntegrationOverflowSpill(t *testing.T) {
	// Two workers, cap-1 each, paused. Both submits should find a home via spill;
	// the third overflows.
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Overflow: OverflowSpill, Worker: tinyQueue}, 2, 0)
	defer r.close()
	r.d.gate.Lock()
	defer r.d.gate.Unlock()
	r.invoke(t, "start")
	id := r.d.pool()

	first, err := r.d.pools.Submit(id, 1, []byte("a"))
	if err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	second, err := r.d.pools.Submit(id, 2, []byte("b"))
	if err != nil {
		t.Fatalf("submit 2 (spill): %v", err)
	}
	if first == second {
		t.Fatalf("spill did not move to a second worker: both %d", first)
	}
	if _, err := r.d.pools.Submit(id, 3, []byte("c")); err != ErrPoolBackpressure {
		t.Fatalf("submit 3 = %v, want ErrPoolBackpressure", err)
	}
}

func TestIntegrationConsistentHashSticky(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: ConsistentHash, Worker: bigMailbox}, 5, 0)
	defer r.close()
	r.invoke(t, "start")
	id := r.d.pool()

	// The same key always routes to the same worker; distinct keys spread out.
	homes := map[uint64]WorkerID{}
	seen := map[WorkerID]bool{}
	for k := uint64(0); k < 50; k++ {
		wid, err := r.d.pools.SubmitKeyed(id, k, k, []byte("x"))
		if err != nil {
			t.Fatalf("submit key %d: %v", k, err)
		}
		homes[k] = wid
		seen[wid] = true
	}
	for rep := 0; rep < 3; rep++ {
		for k := uint64(0); k < 50; k++ {
			wid, err := r.d.pools.SubmitKeyed(id, k, k, []byte("x"))
			if err != nil {
				t.Fatalf("resubmit key %d: %v", k, err)
			}
			if wid != homes[k] {
				t.Fatalf("key %d routed to %d, previously %d — not sticky", k, wid, homes[k])
			}
		}
	}
	if len(seen) < 2 {
		t.Fatalf("consistent hash used only %d workers; expected spread", len(seen))
	}
}

func TestIntegrationOnEvent(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Worker: bigMailbox}, 3, 0)
	defer r.close()

	var mu sync.Mutex
	kinds := map[PoolEventKind]int{}
	r.d.pools.OnEvent(func(ev *PoolEvent) {
		mu.Lock()
		kinds[ev.Kind]++
		mu.Unlock()
	})

	r.invoke(t, "start")
	id := r.d.pool()
	for i := 0; i < 10; i++ {
		if _, err := r.d.pools.Submit(id, uint64(i), []byte("e")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "events to arrive", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return kinds[PoolCreated] == 1 && kinds[PoolWorkerAdded] == 3 && kinds[PoolTaskRouted] == 10
	})

	if err := r.d.pools.Destroy(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "destroy event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return kinds[PoolDestroyed] == 1
	})
}

func TestIntegrationSubmitFromHeals(t *testing.T) {
	// Elastic pool [1,4]. Poison the lone worker, then pump: SubmitFrom must
	// reconcile the pool back up and route the task.
	r := newRig(t, PoolOptions{Strategy: RoundRobin, MaxWorkers: 4, TargetPerWorker: 1, Worker: bigMailbox}, 1, 0)
	defer r.close()
	r.d.mu.Lock()
	r.d.trapTag = 0xC0FFEE
	r.d.mu.Unlock()
	r.invoke(t, "start")
	id := r.d.pool()

	if _, err := r.d.pools.Submit(id, 0xC0FFEE, []byte("boom")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "lone worker to die", func() bool { return r.stats(t).Live == 0 })

	// pump == SubmitFrom: reconcile (respawn to the floor) then route.
	waitFor(t, "SubmitFrom to heal and route", func() bool {
		r.invoke(t, "pump")
		st := r.stats(t)
		return st.Live >= 1 && st.Routed >= 1
	})
}

func TestIntegrationCreateWorkerLimit(t *testing.T) {
	// MaxWorkersPerPool below the requested MinWorkers rejects creation.
	r := newRigLim(t, PoolOptions{Strategy: RoundRobin}, 8, 0, Limits{MaxWorkersPerPool: 4})
	defer r.close()
	r.invoke(t, "start")
	if len(r.d.pools_()) != 0 {
		t.Fatalf("pool created despite worker limit: %v", r.d.pools_())
	}
	if len(r.d.createErrs) != 1 || r.d.createErrs[0] != ErrPoolWorkerLimit {
		t.Fatalf("create errs = %v, want [ErrPoolWorkerLimit]", r.d.createErrs)
	}
}

func TestIntegrationErrorsUnknownPool(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin}, 1, 0)
	defer r.close()
	r.invoke(t, "start")
	const bogus PoolID = 999999

	if _, err := r.d.pools.Submit(bogus, 1, nil); err != ErrPoolNotFound {
		t.Fatalf("submit unknown = %v, want ErrPoolNotFound", err)
	}
	if _, err := r.d.pools.SubmitKeyed(bogus, 1, 1, nil); err != ErrPoolNotFound {
		t.Fatalf("submitkeyed unknown = %v, want ErrPoolNotFound", err)
	}
	if _, err := r.d.pools.Broadcast(bogus, 1, nil); err != ErrPoolNotFound {
		t.Fatalf("broadcast unknown = %v, want ErrPoolNotFound", err)
	}
	if err := r.d.pools.Drain(bogus); err != ErrPoolNotFound {
		t.Fatalf("drain unknown = %v, want ErrPoolNotFound", err)
	}
	if err := r.d.pools.Destroy(bogus); err != ErrPoolNotFound {
		t.Fatalf("destroy unknown = %v, want ErrPoolNotFound", err)
	}
	if _, err := r.d.pools.Stats(bogus); err != ErrPoolNotFound {
		t.Fatalf("stats unknown = %v, want ErrPoolNotFound", err)
	}
}

func TestIntegrationBroadcastPartialAndCounts(t *testing.T) {
	// Paused workers with cap-1 mailboxes: first broadcast fills every mailbox,
	// the second finds them all full and is fully rejected.
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Worker: tinyQueue}, 3, 0)
	defer r.close()
	r.d.gate.Lock()
	defer r.d.gate.Unlock()
	r.invoke(t, "start")
	id := r.d.pool()

	if n, err := r.d.pools.Broadcast(id, 1, []byte("x")); err != nil || n != 3 {
		t.Fatalf("first broadcast = (%d,%v), want (3,nil)", n, err)
	}
	if n, err := r.d.pools.Broadcast(id, 2, []byte("y")); err != nil || n != 0 {
		t.Fatalf("second broadcast = (%d,%v), want (0,nil)", n, err)
	}
	st := r.stats(t)
	if st.Routed != 3 || st.Rejected != 3 {
		t.Fatalf("broadcast counts: routed=%d rejected=%d, want 3,3", st.Routed, st.Rejected)
	}
}

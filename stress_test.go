package pool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wago-org/wago"
	"github.com/wago-org/workers"
)

// bigMailbox gives workers deep mailboxes so a stress burst never overflows and
// every routed task is delivered. The per-worker byte reservation stays modest
// (payloads are tiny) so many workers fit under the workers service's aggregate
// queue-byte budget.
var bigMailbox = WorkerOptions{QueueCapacity: 1 << 14, MaxPayloadBytes: 1 << 10, MaxQueueBytes: 1 << 20}

func scale(t *testing.T, full int) int {
	if testing.Short() {
		return full / 10
	}
	return full
}

// TestStressConcurrentSubmit hammers one pool from many producers at once and
// verifies every task is accounted for and delivered exactly once, with the pool
// size stable throughout.
func TestStressConcurrentSubmit(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: LeastLoaded, Worker: bigMailbox}, 8, 0)
	defer r.close()
	r.invoke(t, "start")
	id := r.d.pool()

	producers := 16
	per := scale(t, 1000)
	total := producers * per

	var wg sync.WaitGroup
	var accepted int64
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := r.d.pools.Submit(id, uint64(base+i), []byte("x")); err == nil {
					atomic.AddInt64(&accepted, 1)
				}
			}
		}(p * per)
	}
	wg.Wait()

	if int(accepted) != total {
		t.Fatalf("accepted %d of %d submits", accepted, total)
	}
	waitFor(t, "all tasks delivered", func() bool { return r.d.messages() == total })

	st := r.stats(t)
	if st.Routed != uint64(total) || st.Rejected != 0 {
		t.Fatalf("routed=%d rejected=%d, want %d,0", st.Routed, st.Rejected, total)
	}
	if st.Live != 8 {
		t.Fatalf("pool size drifted to %d, want 8", st.Live)
	}
	// Least-loaded should spread work; no worker should have been starved.
	for wid, c := range r.d.distribution() {
		if c == 0 {
			t.Fatalf("worker %d received no work", wid)
		}
	}
}

// TestStressChurnAndHeal runs continuous traffic while repeatedly poisoning
// workers and reconciling, asserting the pool self-heals to full strength and
// the service never deadlocks.
func TestStressChurnAndHeal(t *testing.T) {
	r := newRig(t, PoolOptions{
		Strategy: RoundRobin, Restart: RestartOnFailure,
		MaxRestarts: UnlimitedRestarts, Worker: bigMailbox,
	}, 6, 0)
	defer r.close()

	r.d.mu.Lock()
	r.d.trapTag = 0xBADF00D
	r.d.mu.Unlock()
	r.invoke(t, "start")
	id := r.d.pool()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					_, _ = r.d.pools.Submit(id, uint64(i), []byte("work"))
				}
			}
		}()
	}

	iters := scale(t, 40)
	for it := 0; it < iters; it++ {
		// Poison one worker, then heal from a live caller.
		_, _ = r.d.pools.Submit(id, r.d.trapTag, []byte("boom"))
		time.Sleep(time.Millisecond)
		r.invoke(t, "reconcile")
	}
	close(stop)
	wg.Wait()

	// Final reconcile settles the pool back to full strength.
	waitFor(t, "pool to heal to full strength", func() bool {
		r.invoke(t, "reconcile")
		return r.stats(t).Live == 6
	})
	st := r.stats(t)
	if st.Failed == 0 {
		t.Fatal("expected worker failures during churn")
	}
	if st.Halted {
		t.Fatal("pool halted despite unlimited restarts")
	}
	// The service is still fully responsive after the churn.
	if _, err := r.d.pools.Submit(id, 1, []byte("after")); err != nil {
		t.Fatalf("submit after churn: %v", err)
	}
}

// TestStressManyPools creates many pools at once, drives traffic to all of them
// concurrently, and tears them all down, checking global bookkeeping stays sane.
func TestStressManyPools(t *testing.T) {
	const workersEach = 6
	npools := scale(t, 8)
	if npools < 2 {
		npools = 2
	}
	r := newRig(t, PoolOptions{Strategy: PowerOfTwo, Worker: bigMailbox}, workersEach, 0)
	defer r.close()

	for i := 0; i < npools; i++ {
		r.invoke(t, "start") // each start creates a fresh pool
	}
	ids := r.d.pools_()
	if len(ids) != npools {
		t.Fatalf("created %d pools, want %d; errs=%v", len(ids), npools, r.d.createErrs)
	}
	if got := len(r.d.pools.List()); got != npools {
		t.Fatalf("List() = %d pools, want %d", got, npools)
	}

	per := scale(t, 400)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id PoolID) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				_, _ = r.d.pools.Submit(id, uint64(i), []byte("m"))
			}
		}(id)
	}
	wg.Wait()

	waitFor(t, "all pools to drain their work", func() bool {
		for _, id := range ids {
			st, err := r.d.pools.Stats(id)
			if err != nil || st.Outstanding != 0 {
				return false
			}
		}
		return true
	})

	// Destroy every pool concurrently; the registry must end empty.
	var dwg sync.WaitGroup
	for _, id := range ids {
		dwg.Add(1)
		go func(id PoolID) {
			defer dwg.Done()
			_ = r.d.pools.Destroy(id)
		}(id)
	}
	dwg.Wait()
	if got := len(r.d.pools.List()); got != 0 {
		t.Fatalf("List() = %d after destroy, want 0", got)
	}
}

// TestStressSubmitDuringDestroy races submitters against a pool being destroyed,
// asserting the service degrades cleanly (no panic, no deadlock) rather than
// corrupting state.
func TestStressSubmitDuringDestroy(t *testing.T) {
	for round := 0; round < scale(t, 20); round++ {
		r := newRig(t, PoolOptions{Strategy: RoundRobin, Worker: bigMailbox}, 8, 0)
		r.invoke(t, "start")
		id := r.d.pool()

		var wg sync.WaitGroup
		for p := 0; p < 8; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					// Any error (drain/not-found) is acceptable; a crash is not.
					_, _ = r.d.pools.Submit(id, uint64(i), []byte("x"))
				}
			}()
		}
		time.Sleep(time.Millisecond)
		_ = r.d.pools.Destroy(id)
		wg.Wait()

		if _, err := r.d.pools.Stats(id); err != ErrPoolNotFound {
			t.Fatalf("round %d: stats after destroy = %v, want ErrPoolNotFound", round, err)
		}
		r.close()
	}
}

// TestStressMixedStrategies exercises every routing strategy end-to-end under
// concurrent load in one runtime, guarding against strategy-specific races.
func TestStressMixedStrategies(t *testing.T) {
	strategies := []Strategy{RoundRobin, LeastLoaded, Random, PowerOfTwo, ConsistentHash}
	per := scale(t, 500)
	for _, s := range strategies {
		s := s
		t.Run(strategyName(s), func(t *testing.T) {
			r := newRig(t, PoolOptions{Strategy: s, Worker: bigMailbox}, 8, 0)
			defer r.close()
			r.invoke(t, "start")
			id := r.d.pool()

			var wg sync.WaitGroup
			for p := 0; p < 8; p++ {
				wg.Add(1)
				go func(base int) {
					defer wg.Done()
					for i := 0; i < per; i++ {
						_, _ = r.d.pools.SubmitKeyed(id, uint64(base+i), uint64(i), []byte("k"))
					}
				}(p * per)
			}
			wg.Wait()

			total := 8 * per
			waitFor(t, "delivery", func() bool { return r.d.messages() == total })
			if st := r.stats(t); st.Routed != uint64(total) {
				t.Fatalf("%s: routed=%d, want %d", strategyName(s), st.Routed, total)
			}
		})
	}
}

func strategyName(s Strategy) string {
	switch s {
	case RoundRobin:
		return "RoundRobin"
	case LeastLoaded:
		return "LeastLoaded"
	case Random:
		return "Random"
	case PowerOfTwo:
		return "PowerOfTwo"
	case ConsistentHash:
		return "ConsistentHash"
	case Broadcast:
		return "Broadcast"
	}
	return "unknown"
}

// TestStressRuntimeCloseWhileBusy ensures shutting the runtime down mid-flight
// tears everything down without hanging.
func TestStressRuntimeCloseWhileBusy(t *testing.T) {
	r := newRig(t, PoolOptions{Strategy: RoundRobin, Worker: bigMailbox}, 12, 0)
	r.invoke(t, "start")
	id := r.d.pool()

	var stop atomic.Bool
	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				_, _ = r.d.pools.Submit(id, uint64(i), []byte("busy"))
			}
		}()
	}
	done := make(chan struct{})
	go func() { r.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		stop.Store(true)
		wg.Wait()
		t.Fatal("runtime close hung while busy")
	}
	// Keep producers racing with shutdown itself: close must revoke admission,
	// drain accepted work, and return without requiring callers to stop first.
	stop.Store(true)
	wg.Wait()
	if err := r.d.poolRef.With(func(Service) error { return nil }); !errors.Is(err, wago.ErrPermissionDenied) {
		t.Fatalf("pool contract after close = %v", err)
	}
	if err := r.d.workers.With(func(workers.Service) error { return nil }); !errors.Is(err, wago.ErrPermissionDenied) {
		t.Fatalf("Workers contract after close = %v", err)
	}
	messages := r.d.messages()
	time.Sleep(10 * time.Millisecond)
	if got := r.d.messages(); got != messages {
		t.Fatalf("late callback after close: messages changed from %d to %d", messages, got)
	}
}

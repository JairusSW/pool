package pool

import (
	"testing"
)

func TestRingRoutesAndIsStable(t *testing.T) {
	r := newRing(12345, 0) // default replicas
	if r.replicas != DefaultRingReplicas {
		t.Fatalf("replicas = %d, want %d", r.replicas, DefaultRingReplicas)
	}
	if _, ok := r.get(1); ok {
		t.Fatal("empty ring returned a worker")
	}
	for id := WorkerID(1); id <= 4; id++ {
		r.add(id)
	}
	if r.len() != 4*int(DefaultRingReplicas) {
		t.Fatalf("ring len = %d, want %d", r.len(), 4*int(DefaultRingReplicas))
	}

	// Same key always maps to the same worker (deterministic routing).
	route := map[uint64]WorkerID{}
	for k := uint64(0); k < 1000; k++ {
		id, ok := r.get(k)
		if !ok || id < 1 || id > 4 {
			t.Fatalf("get(%d) = %d, %v", k, id, ok)
		}
		route[k] = id
	}
	for k := uint64(0); k < 1000; k++ {
		if id, _ := r.get(k); id != route[k] {
			t.Fatalf("get(%d) not stable: %d != %d", k, id, route[k])
		}
	}

	// Adding a 5th worker only remaps a minority of keys (consistent hashing),
	// never a wholesale reshuffle.
	r.add(5)
	moved := 0
	for k := uint64(0); k < 1000; k++ {
		if id, _ := r.get(k); id != route[k] {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("adding a worker moved no keys")
	}
	if moved > 500 {
		t.Fatalf("adding a worker moved %d/1000 keys; expected a minority", moved)
	}

	// Removing a worker sends its keys elsewhere but leaves others put.
	before := map[uint64]WorkerID{}
	for k := uint64(0); k < 1000; k++ {
		before[k], _ = r.get(k)
	}
	r.remove(3)
	if r.len() != 4*int(DefaultRingReplicas) {
		t.Fatalf("after remove len = %d", r.len())
	}
	for k := uint64(0); k < 1000; k++ {
		id, _ := r.get(k)
		if id == 3 {
			t.Fatalf("removed worker 3 still routed for key %d", k)
		}
		if before[k] != 3 && id != before[k] {
			t.Fatalf("key %d moved from %d to %d though its worker stayed", k, before[k], id)
		}
	}
}

func TestRingDistribution(t *testing.T) {
	r := newRing(99, 128)
	const workers = 8
	for id := WorkerID(1); id <= workers; id++ {
		r.add(id)
	}
	counts := map[WorkerID]int{}
	const keys = 80000
	for k := uint64(0); k < keys; k++ {
		id, _ := r.get(k)
		counts[id]++
	}
	// With 128 virtual nodes per worker the load should be within ~2x of even.
	avg := keys / workers
	for id := WorkerID(1); id <= workers; id++ {
		c := counts[id]
		if c < avg/2 || c > avg*2 {
			t.Errorf("worker %d got %d keys, avg %d — distribution too skewed", id, c, avg)
		}
	}
}

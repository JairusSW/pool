package pool

import "sort"

// DefaultRingReplicas is the number of virtual nodes placed on the hash ring per
// worker when PoolOptions.RingReplicas is left zero. More replicas smooth the key
// distribution across workers at the cost of a larger ring; 64 keeps a handful of
// workers well balanced while staying cheap to rebuild on membership change.
const DefaultRingReplicas uint32 = 64

// splitmix64 is a fast, well-distributed finalizer used both to place virtual
// nodes on the ring and to hash routing keys onto it. It is deterministic, so
// routing is reproducible for a given seed and membership.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// mix2 hashes a pair into one 64-bit point; used to derive each worker's virtual
// node positions (mix2(seed^id, replicaIndex)).
func mix2(a, b uint64) uint64 { return splitmix64(a ^ splitmix64(b)) }

// vnode is one virtual node: a point on the 64-bit ring owned by a worker.
type vnode struct {
	point uint64
	id    WorkerID
}

// ring is a virtual-node consistent-hash ring. It maps a routing key to a stable
// worker so the same key lands on the same worker across submits, and so growing
// or shrinking the pool only remaps the keys near the changed worker rather than
// reshuffling every key. It is not safe for concurrent use; the owning pool holds
// its mutex around every ring operation.
type ring struct {
	seed     uint64
	replicas uint32
	nodes    []vnode // sorted ascending by point
}

func newRing(seed uint64, replicas uint32) *ring {
	if replicas == 0 {
		replicas = DefaultRingReplicas
	}
	return &ring{seed: seed, replicas: replicas}
}

func (r *ring) add(id WorkerID) {
	base := r.seed ^ splitmix64(uint64(id))
	for i := uint32(0); i < r.replicas; i++ {
		r.nodes = append(r.nodes, vnode{point: mix2(base, uint64(i)), id: id})
	}
	sort.Slice(r.nodes, func(i, j int) bool { return r.nodes[i].point < r.nodes[j].point })
}

func (r *ring) remove(id WorkerID) {
	kept := r.nodes[:0]
	for _, n := range r.nodes {
		if n.id != id {
			kept = append(kept, n)
		}
	}
	// Zero the tail so removed WorkerIDs are not retained by the backing array.
	for i := len(kept); i < len(r.nodes); i++ {
		r.nodes[i] = vnode{}
	}
	r.nodes = kept
}

// get returns the worker owning the first virtual node at or after hash(key),
// wrapping around the ring. ok is false only when the ring is empty.
func (r *ring) get(key uint64) (WorkerID, bool) {
	if len(r.nodes) == 0 {
		return 0, false
	}
	h := splitmix64(key)
	i := sort.Search(len(r.nodes), func(i int) bool { return r.nodes[i].point >= h })
	if i == len(r.nodes) {
		i = 0
	}
	return r.nodes[i].id, true
}

func (r *ring) len() int { return len(r.nodes) }

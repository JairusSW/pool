// Package pool provides elastic, load-balanced, self-healing pools of Wago
// workers as an optional Wago plugin.
//
// A pool is a named group of workers that all run the same guest table entry.
// The pool routes tasks (tagged byte payloads) across its members with a chosen
// load-balancing strategy, keeps the pool at its target size by respawning or
// scaling workers, and can grow and shrink with load. It builds entirely on the
// primitives of the github.com/wago-org/workers plugin — Spawn, Send,
// DispatchNext, Link, Kill, plus owned message and exit subscriptions — and adds
// the policy layer that workers deliberately leaves out: routing, supervision,
// autoscaling, draining, and per-pool metrics.
//
// Pool requests no Wago Authorities of its own. Its explicit package dependency
// selects Workers, and its typed Contract reference keeps every cross-plugin
// call and shutdown dependency inside a revocable callback lease. See the README
// for the full model.
package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/wago-org/wago"
	wagoplugin "github.com/wago-org/wago/plugin"
	"github.com/wago-org/workers"
)

// OptimalWorkers returns the recommended ceiling on simultaneously busy workers
// for CPU-bound wasm work on this machine: GOMAXPROCS, at least 1. Workers run
// their guest on their own goroutines, so once as many workers are churning as
// the runtime can execute in parallel, adding more does not add throughput — it
// adds context switching, scheduler contention, and cache pressure that
// deteriorate performance. An elastic pool that leaves PoolOptions.MaxWorkers
// unset uses this as its ceiling, and the service's RunnableWorkers budget uses
// it to keep pools from collectively oversubscribing the CPU.
func OptimalWorkers() uint32 {
	if n := runtime.GOMAXPROCS(0); n > 0 {
		return uint32(n)
	}
	return 1
}

const PluginID = "github.com/JairusSW/pool"

// WorkerID and WorkerOptions are re-exported from the workers plugin for the
// convenience of callers that only import pool.
type (
	WorkerID      = workers.WorkerID
	WorkerOptions = workers.WorkerOptions
)

// PoolID identifies a pool within one Pools service.
type PoolID uint64

// Strategy selects how a pool routes each task to one of its workers.
type Strategy uint8

const (
	// RoundRobin cycles through workers in order, spreading tasks evenly.
	RoundRobin Strategy = iota + 1
	// LeastLoaded routes to the worker with the smallest mailbox backlog.
	LeastLoaded
	// Random routes to a uniformly random worker.
	Random
	// PowerOfTwo samples two random workers and picks the less loaded of the
	// two — near-least-loaded quality at O(1) cost, and provably good under load.
	PowerOfTwo
	// ConsistentHash routes by a key (the task key) to a stable worker via a
	// virtual-node hash ring, so the same key lands on the same worker and
	// scaling remaps only a fraction of keys.
	ConsistentHash
	// Broadcast delivers every task to every worker in the pool.
	Broadcast
)

func (s Strategy) valid() bool { return s >= RoundRobin && s <= Broadcast }

// OverflowPolicy decides what a routed task does when its chosen worker's
// mailbox is full.
type OverflowPolicy uint8

const (
	// OverflowSpill tries the remaining workers in preference order before
	// giving up. This is the default.
	OverflowSpill OverflowPolicy = iota + 1
	// OverflowReject fails fast with ErrPoolBackpressure if the chosen worker is
	// full, without trying others.
	OverflowReject
	// OverflowShed silently drops the task (counting it) when no worker can take
	// it, returning (0, nil). Use for best-effort, loss-tolerant fan-in.
	OverflowShed
)

func (o OverflowPolicy) valid() bool { return o >= OverflowSpill && o <= OverflowShed }

// RestartPolicy governs whether a fixed-size pool (one with autoscaling off)
// replaces a worker that exits. Elastic pools (TargetPerWorker > 0) always
// self-heal toward their load-derived target and use this only for crash-loop
// accounting on failures.
type RestartPolicy uint8

const (
	// RestartOnFailure replaces a worker that failed (trapped) or was killed, but
	// not one that returned cleanly. This is the default.
	RestartOnFailure RestartPolicy = iota + 1
	// RestartAlways replaces every worker that exits, keeping the pool full even
	// when workers finish their table entry and return.
	RestartAlways
	// RestartNever never replaces an exited worker; the pool shrinks permanently.
	RestartNever
)

func (r RestartPolicy) valid() bool { return r >= RestartOnFailure && r <= RestartNever }

// UnlimitedRestarts, used as PoolOptions.MaxRestarts, disables crash-loop
// protection so a pool refills failed workers without bound.
const UnlimitedRestarts uint32 = ^uint32(0)

// Package defaults for PoolOptions and Limits.
const (
	DefaultMaxRestarts uint32 = 16

	DefaultMaxPools          uint32 = 64
	DefaultMaxWorkersPerPool uint32 = 64
	DefaultMaxTotalWorkers   uint32 = 256
)

type poolError string

func (e poolError) Error() string { return string(e) }

// Errors returned by the Pools service. All are comparable sentinels; match them
// with errors.Is.
const (
	ErrPoolsInactive      poolError = "pool service is not active"
	ErrPoolsClosed        poolError = "pool service is closed"
	ErrWorkersUnavailable poolError = "workers service is unavailable"
	ErrPoolNotFound       poolError = "pool not found"
	ErrPoolEmpty          poolError = "pool has no live workers"
	ErrPoolDraining       poolError = "pool is draining"
	ErrPoolBackpressure   poolError = "all candidate workers are at capacity"
	ErrInvalidPoolOptions poolError = "invalid pool options"
	ErrPoolLimit          poolError = "pool count limit reached"
	ErrPoolWorkerLimit    poolError = "pool worker limit reached"
	ErrPoolIDExhausted    poolError = "pool ID space exhausted"
)

// Limits bounds the aggregate resources one Pools service may hold, independent
// of the per-pool caps. Zero fields take the package defaults. These caps sit
// above — and are additionally bounded by — the workers service's own
// WorkerLimits, which is the hard ceiling on live workers.
type Limits struct {
	// MaxPools is the maximum number of pools that may exist at once.
	MaxPools uint32
	// MaxWorkersPerPool caps the workers in any single pool, overriding a larger
	// PoolOptions.MaxWorkers.
	MaxWorkersPerPool uint32
	// MaxTotalWorkers caps the workers summed across all pools.
	MaxTotalWorkers uint32
	// RunnableWorkers bounds how many workers autoscaling will run across all
	// pools at once, so elastic pools do not collectively oversubscribe the CPU
	// and deteriorate throughput. Zero takes OptimalWorkers() — the machine's
	// parallelism. It caps autoscale growth only: a pool's MinWorkers floor and any
	// size set explicitly with Scale are always honored, even beyond this budget.
	RunnableWorkers uint32
}

func normalizeLimits(l Limits) Limits {
	if l.MaxPools == 0 {
		l.MaxPools = DefaultMaxPools
	}
	if l.MaxWorkersPerPool == 0 {
		l.MaxWorkersPerPool = DefaultMaxWorkersPerPool
	}
	if l.MaxTotalWorkers == 0 {
		l.MaxTotalWorkers = DefaultMaxTotalWorkers
	}
	if l.RunnableWorkers == 0 {
		l.RunnableWorkers = OptimalWorkers()
	}
	return l
}

// PoolOptions configures one pool at creation. Zero fields take sensible
// defaults (see the field docs). MinWorkers workers are spawned immediately.
type PoolOptions struct {
	// Strategy selects the routing algorithm. Default RoundRobin.
	Strategy Strategy
	// Overflow decides what a task does when its chosen worker is full. Default
	// OverflowSpill.
	Overflow OverflowPolicy
	// Restart governs replacement of exited workers in a fixed-size pool. Default
	// RestartOnFailure.
	Restart RestartPolicy
	// MinWorkers is both the initial pool size and the autoscale floor. Default 1.
	MinWorkers uint32
	// MaxWorkers is the autoscale ceiling. When zero: an autoscaling pool
	// (TargetPerWorker > 0) sizes the ceiling to OptimalWorkers() — the machine's
	// parallelism — so it fills the CPU without oversubscribing it; a pool with no
	// autoscaling stays fixed at MinWorkers.
	MaxWorkers uint32
	// TargetPerWorker turns on autoscaling: the pool keeps roughly this many
	// outstanding tasks per worker, growing toward MaxWorkers under load and back
	// toward MinWorkers when idle. Zero disables autoscaling.
	TargetPerWorker uint32
	// MaxRestarts caps total worker replacements over a pool's life; exceeding it
	// halts replacement (crash-loop protection). Zero takes DefaultMaxRestarts;
	// use UnlimitedRestarts to disable the cap.
	MaxRestarts uint32
	// Worker bounds each worker's mailbox (see workers.WorkerOptions). Zero fields
	// take the workers-package defaults.
	Worker WorkerOptions
	// HashSeed seeds the ConsistentHash ring and the Random/PowerOfTwo generators.
	// Zero derives a seed from the PoolID, so routing is reproducible per pool.
	HashSeed uint64
	// RingReplicas is the virtual nodes per worker on the ConsistentHash ring.
	// Zero takes DefaultRingReplicas.
	RingReplicas uint32
}

func (o PoolOptions) seedFor(id PoolID) uint64 {
	if o.HashSeed != 0 {
		return o.HashSeed
	}
	return splitmix64(uint64(id))
}

func normalizePoolOptions(o PoolOptions) (PoolOptions, error) {
	if o.Strategy == 0 {
		o.Strategy = RoundRobin
	}
	if o.Overflow == 0 {
		o.Overflow = OverflowSpill
	}
	if o.Restart == 0 {
		o.Restart = RestartOnFailure
	}
	if o.MinWorkers == 0 {
		o.MinWorkers = 1
	}
	if o.MaxWorkers == 0 {
		if o.TargetPerWorker > 0 {
			// Autoscaling requested with no explicit ceiling: size it to the machine
			// so the pool grows to fill the CPU but never oversubscribes it.
			o.MaxWorkers = OptimalWorkers()
			if o.MaxWorkers < o.MinWorkers {
				o.MaxWorkers = o.MinWorkers
			}
		} else {
			o.MaxWorkers = o.MinWorkers // fixed pool
		}
	}
	if o.MaxRestarts == 0 {
		o.MaxRestarts = DefaultMaxRestarts
	}
	if !o.Strategy.valid() || !o.Overflow.valid() || !o.Restart.valid() || o.MaxWorkers < o.MinWorkers {
		return PoolOptions{}, ErrInvalidPoolOptions
	}
	return o, nil
}

// WorkerStat is a per-worker snapshot within PoolStats.
type WorkerStat struct {
	ID          WorkerID
	Enqueued    uint64 // tasks the pool has routed to this worker
	Dispatched  uint64 // tasks the worker has pulled from its mailbox
	Outstanding uint32 // Enqueued - Dispatched: current mailbox backlog
}

// PoolStats is an atomic snapshot of a pool's size, configuration, and counters.
type PoolStats struct {
	Pool        PoolID
	Strategy    Strategy
	Live        uint32 // workers currently in the pool
	Desired     uint32 // target size the pool is converging toward
	Min, Max    uint32 // autoscale band from PoolOptions
	Draining    bool
	Halted      bool   // crash-loop protection has stopped replacement
	Submitted   uint64 // Submit/Broadcast calls accepted
	Routed      uint64 // tasks successfully handed to a worker
	Rejected    uint64 // tasks that no worker could accept
	Shed        uint64 // tasks dropped under OverflowShed
	Restarted   uint64 // worker replacements decided
	Exited      uint64 // worker exits observed
	Failed      uint64 // worker exits that were failures (traps)
	Outstanding uint64 // sum of per-worker backlog
	Workers     []WorkerStat
}

// PoolEventKind classifies a PoolEvent.
type PoolEventKind uint8

const (
	PoolCreated PoolEventKind = iota + 1
	PoolWorkerAdded
	PoolWorkerRemoved
	PoolScaled
	PoolTaskRouted
	PoolTaskRejected
	PoolDrained
	PoolDestroyed
)

// PoolEvent is delivered to observers registered with OnEvent. It is a
// fire-and-forget notification for logging, metrics, or external orchestration;
// observers must not call back into the same Pools operation synchronously in a
// way that blocks, but they may read Stats or submit work.
type PoolEvent struct {
	Kind     PoolEventKind
	Pool     PoolID
	Worker   WorkerID // 0 when not worker-specific
	ExitKind workers.WorkerExitKind
	Err      error
}

// Service is Pool's major-versioned cross-plugin API.
type Service interface {
	Create(wago.HostModule, uint32, PoolOptions) (PoolID, error)
	Submit(PoolID, uint64, []byte) (WorkerID, error)
	SubmitKeyed(PoolID, uint64, uint64, []byte) (WorkerID, error)
	SubmitFrom(wago.HostModule, PoolID, uint64, []byte) (WorkerID, error)
	Broadcast(PoolID, uint64, []byte) (int, error)
	Reconcile(wago.HostModule, PoolID) error
	Scale(wago.HostModule, PoolID, uint32) error
	Drain(PoolID) error
	Destroy(PoolID) error
	Stats(PoolID) (PoolStats, error)
	List() []PoolID
	ObserveEvents(func(*PoolEvent)) (EventSubscription, error)
	UnsubscribeEvents(EventSubscription) error
}

var Contract = wagoplugin.NewContract[Service](PluginID+"/service", 1)

type pluginConfig struct {
	MaxPools          *uint32 `json:"maxPools,omitempty"`
	MaxWorkersPerPool *uint32 `json:"maxWorkersPerPool,omitempty"`
	MaxTotalWorkers   *uint32 `json:"maxTotalWorkers,omitempty"`
	RunnableWorkers   *uint32 `json:"runnableWorkers,omitempty"`
}

var configSchema = json.RawMessage(`{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "maxPools": {"type": "integer", "minimum": 1, "maximum": 65536},
    "maxWorkersPerPool": {"type": "integer", "minimum": 1, "maximum": 65536},
    "maxTotalWorkers": {"type": "integer", "minimum": 1, "maximum": 1048576},
    "runnableWorkers": {"type": "integer", "minimum": 1, "maximum": 1048576}
  }
}`)

func decodePluginConfig(raw json.RawMessage) (pluginConfig, Limits, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := validateConfigObject(raw); err != nil {
		return pluginConfig{}, Limits{}, fmt.Errorf("pool: config: %w", err)
	}
	var cfg pluginConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return pluginConfig{}, Limits{}, fmt.Errorf("pool: config: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return pluginConfig{}, Limits{}, fmt.Errorf("pool: config has a trailing JSON value")
	}
	limits := normalizeLimits(Limits{})
	if cfg.MaxPools != nil {
		limits.MaxPools = *cfg.MaxPools
	}
	if cfg.MaxWorkersPerPool != nil {
		limits.MaxWorkersPerPool = *cfg.MaxWorkersPerPool
	}
	if cfg.MaxTotalWorkers != nil {
		limits.MaxTotalWorkers = *cfg.MaxTotalWorkers
	}
	if cfg.RunnableWorkers != nil {
		limits.RunnableWorkers = *cfg.RunnableWorkers
	}
	if limits.MaxPools == 0 || limits.MaxPools > 65536 || limits.MaxWorkersPerPool == 0 || limits.MaxWorkersPerPool > 65536 ||
		limits.MaxTotalWorkers == 0 || limits.MaxTotalWorkers > 1<<20 || limits.RunnableWorkers == 0 || limits.RunnableWorkers > 1<<20 ||
		limits.MaxWorkersPerPool > limits.MaxTotalWorkers {
		return pluginConfig{}, Limits{}, fmt.Errorf("pool: config limits are invalid")
	}
	return cfg, limits, nil
}

func validateConfigObject(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("must be a JSON object")
	}
	seen := map[string]struct{}{}
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("object key is not a string")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("field %q must not be null", key)
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("has a trailing JSON value")
		}
		return err
	}
	return nil
}

func Definition() wago.PluginDefinition {
	return wago.PluginDefinition{
		ID: PluginID, Name: "Pool", Version: "0.1.0",
		Description:   "Elastic, load-balanced, self-healing pools of Wago workers.",
		Stability:     wago.Experimental,
		Compatibility: wago.Compatibility{Engines: map[string]string{"wago": ">=0.1.0"}},
		Provenance: wago.PluginProvenance{
			Homepage: "https://github.com/JairusSW/pool", Repository: "https://github.com/JairusSW/pool", License: "Apache-2.0",
			Authors: []string{"Jairus Tanaka"},
		},
		Requires: []wago.PluginRequirement{{ID: workers.PluginID, Version: "^0.1.0"}},
		Consumes: []wago.ContractRequirement{{ID: workers.Contract.ID(), Major: workers.Contract.Major(), Mode: wago.ContractRequired}},
		Provides: []wago.ContractSpec{Contract.Spec()}, ConfigSchema: append(json.RawMessage(nil), configSchema...),
	}
}

func Provider() wago.PluginProvider {
	return wago.PluginProvider{
		Definition: Definition(), New: func() wago.Plugin { return new(plugin) },
		ValidateConfig: func(raw json.RawMessage) error { _, _, err := decodePluginConfig(raw); return err },
	}
}

type plugin struct{ service *Pools }

func (p *plugin) Register(reg *wago.Registrar) error {
	var cfg pluginConfig
	if err := reg.Config(&cfg); err != nil {
		return err
	}
	limits := normalizeLimits(Limits{})
	if cfg.MaxPools != nil {
		limits.MaxPools = *cfg.MaxPools
	}
	if cfg.MaxWorkersPerPool != nil {
		limits.MaxWorkersPerPool = *cfg.MaxWorkersPerPool
	}
	if cfg.MaxTotalWorkers != nil {
		limits.MaxTotalWorkers = *cfg.MaxTotalWorkers
	}
	if cfg.RunnableWorkers != nil {
		limits.RunnableWorkers = *cfg.RunnableWorkers
	}
	ref, err := wagoplugin.Require(reg, workers.Contract)
	if err != nil {
		return err
	}
	p.service = newPools(ref, limits)
	if err := wagoplugin.Provide(reg, Contract, Service(p.service)); err != nil {
		return err
	}
	return reg.Lifecycle(wago.PluginLifecycle{
		Start: func(context.Context) error { return p.service.start() },
		Stop:  func(context.Context) error { return p.service.close() },
	})
}

// Pools implements Pool's typed Service Contract. Consumers access it only
// inside the callback passed to plugin.Ref.With.
type Pools struct {
	ref    *wagoplugin.Ref[workers.Service]
	limits Limits

	wireMu  sync.Mutex
	wired   bool
	message workers.Subscription
	exit    workers.Subscription

	hasObs         atomic.Bool
	obsMu          sync.Mutex
	nextObs        uint64
	obs            map[uint64]*eventObserver
	observerPanics []error

	mu       sync.Mutex
	pools    map[PoolID]*pool
	byWorker map[WorkerID]*pool
	next     PoolID
	total    uint32
	closed   bool
}

func newPools(ref *wagoplugin.Ref[workers.Service], limits Limits) *Pools {
	return &Pools{
		ref: ref, limits: normalizeLimits(limits), next: 1,
		pools: map[PoolID]*pool{}, byWorker: map[WorkerID]*pool{}, obs: map[uint64]*eventObserver{},
	}
}

// member is one worker in a pool.
type member struct {
	id           WorkerID
	enqueued     uint64
	dispatched   uint64
	killWhenIdle bool // set during graceful drain; killed once its mailbox empties
}

func (m *member) outstanding() uint32 {
	if m.enqueued <= m.dispatched {
		return 0
	}
	d := m.enqueued - m.dispatched
	if d > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(d)
}

// pool is one pool's state. All fields are guarded by Pools.mu.
type pool struct {
	id         PoolID
	tableIndex uint32
	wopts      WorkerOptions
	strategy   Strategy
	overflow   OverflowPolicy
	restart    RestartPolicy
	capMin     uint32
	capMax     uint32
	perWorker  uint32
	maxRestart uint32
	seed       uint64

	members  []*member
	byWorker map[WorkerID]*member
	ring     *ring
	cursor   uint32
	rng      uint64

	desired   uint32
	draining  bool
	destroyed bool
	halted    bool

	submitted uint64
	routed    uint64
	rejected  uint64
	shed      uint64
	restarts  uint64
	exited    uint64
	failed    uint64
}

func (pl *pool) autoscale() bool { return pl.perWorker > 0 && pl.capMax > pl.capMin }

func (pl *pool) totalOutstanding() uint64 {
	var sum uint64
	for _, m := range pl.members {
		sum += uint64(m.outstanding())
	}
	return sum
}

func (pl *pool) snapshotIDs() []WorkerID {
	ids := make([]WorkerID, len(pl.members))
	for i, m := range pl.members {
		ids[i] = m.id
	}
	return ids
}

func (pl *pool) rand() uint64 {
	x := pl.rng
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	pl.rng = x
	return x
}

// start resolves the exact reviewed Workers binding and owns both observer
// subscriptions before Pool becomes active.
func (p *Pools) start() error {
	p.wireMu.Lock()
	defer p.wireMu.Unlock()
	if p.wired {
		return nil
	}
	if p.ref == nil {
		return ErrPoolsInactive
	}
	if err := p.ref.With(func(svc workers.Service) error {
		if svc == nil {
			return ErrWorkersUnavailable
		}
		var err error
		p.message, err = svc.ObserveMessages(func(ctx *workers.MessageContext) error { p.onMessage(ctx); return nil })
		if err != nil {
			return err
		}
		p.exit, err = svc.ObserveExits(func(ctx *workers.WorkerExitContext) { p.onExit(ctx) })
		if err != nil {
			_ = svc.Unsubscribe(p.message)
			p.message = workers.Subscription{}
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	p.wired = true
	return nil
}

func (p *Pools) ensureWired() error {
	p.wireMu.Lock()
	wired := p.wired
	p.wireMu.Unlock()
	if !wired {
		return ErrPoolsInactive
	}
	return nil
}

func (p *Pools) withWorkers(fn func(workers.Service) error) error {
	if err := p.ensureWired(); err != nil {
		return err
	}
	if p.ref == nil {
		return ErrWorkersUnavailable
	}
	return p.ref.With(fn)
}

// EventSubscription is an opaque observer token. Pass it back through
// Service.UnsubscribeEvents inside a leased contract call.
type EventSubscription struct{ id uint64 }

type eventObserver struct {
	id       uint64
	fn       func(*PoolEvent)
	mu       sync.Mutex
	cond     *sync.Cond
	active   bool
	inFlight uint32
}

func newEventObserver(id uint64, fn func(*PoolEvent)) *eventObserver {
	o := &eventObserver{id: id, fn: fn, active: true}
	o.cond = sync.NewCond(&o.mu)
	return o
}

func (o *eventObserver) invoke(event *PoolEvent) (panicErr error) {
	o.mu.Lock()
	if !o.active {
		o.mu.Unlock()
		return nil
	}
	o.inFlight++
	o.mu.Unlock()
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				panicErr = fmt.Errorf("pool: event observer %d panicked: %v", o.id, recovered)
			}
		}()
		o.fn(event)
	}()
	o.mu.Lock()
	o.inFlight--
	if o.inFlight == 0 {
		o.cond.Broadcast()
	}
	o.mu.Unlock()
	return panicErr
}

func (o *eventObserver) stop() {
	o.mu.Lock()
	o.active = false
	for o.inFlight != 0 {
		o.cond.Wait()
	}
	o.mu.Unlock()
}

// ObserveEvents registers an observer until its returned Subscription closes.
func (p *Pools) ObserveEvents(fn func(*PoolEvent)) (EventSubscription, error) {
	if p == nil || fn == nil {
		return EventSubscription{}, fmt.Errorf("pool: invalid event observer")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return EventSubscription{}, ErrPoolsClosed
	}
	p.obsMu.Lock()
	p.nextObs++
	if p.nextObs == 0 {
		p.obsMu.Unlock()
		p.mu.Unlock()
		return EventSubscription{}, fmt.Errorf("pool: observer ID space exhausted")
	}
	id := p.nextObs
	observer := newEventObserver(id, fn)
	p.obs[id] = observer
	p.hasObs.Store(true)
	p.obsMu.Unlock()
	p.mu.Unlock()
	return EventSubscription{id: id}, nil
}

// UnsubscribeEvents removes one observer and waits for callbacks already in
// flight. It is idempotent. Do not call it from inside that observer callback.
func (p *Pools) UnsubscribeEvents(subscription EventSubscription) error {
	if p == nil || subscription.id == 0 {
		return fmt.Errorf("pool: invalid event subscription")
	}
	p.obsMu.Lock()
	observer := p.obs[subscription.id]
	delete(p.obs, subscription.id)
	p.hasObs.Store(len(p.obs) != 0)
	p.obsMu.Unlock()
	if observer != nil {
		observer.stop()
	}
	return nil
}

// fire delivers ev to observers. It must never be called while holding p.mu, so
// an observer may call back into the service. It is a no-op (a single atomic
// load) when no observers are registered.
func (p *Pools) fire(ev *PoolEvent) {
	if !p.hasObs.Load() {
		return
	}
	p.obsMu.Lock()
	ids := make([]uint64, 0, len(p.obs))
	for id := range p.obs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	observers := make([]*eventObserver, 0, len(ids))
	for _, id := range ids {
		observers = append(observers, p.obs[id])
	}
	p.obsMu.Unlock()
	for _, observer := range observers {
		if panicErr := observer.invoke(ev); panicErr != nil {
			p.obsMu.Lock()
			p.observerPanics = append(p.observerPanics, panicErr)
			p.obsMu.Unlock()
		}
	}
}

// Create spawns a pool of opts.MinWorkers workers, each running the guest table
// entry at tableIndex (which must have the Wasm signature () -> ()), forked from
// the calling instance. It must be called from inside a live host import, where
// caller is the active wago.HostModule. On any spawn failure the partially built
// pool is destroyed and the error is returned.
func (p *Pools) Create(caller wago.HostModule, tableIndex uint32, opts PoolOptions) (PoolID, error) {
	if err := p.ensureWired(); err != nil {
		return 0, err
	}
	opts, err := normalizePoolOptions(opts)
	if err != nil {
		return 0, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, ErrPoolsClosed
	}
	if uint32(len(p.pools)) >= p.limits.MaxPools {
		p.mu.Unlock()
		return 0, ErrPoolLimit
	}
	if opts.MinWorkers > p.limits.MaxWorkersPerPool {
		p.mu.Unlock()
		return 0, ErrPoolWorkerLimit
	}
	if p.next == 0 {
		p.mu.Unlock()
		return 0, ErrPoolIDExhausted
	}
	id := p.next
	if id == ^PoolID(0) {
		p.next = 0
	} else {
		p.next++
	}
	seed := opts.seedFor(id)
	pl := &pool{
		id: id, tableIndex: tableIndex, wopts: opts.Worker, strategy: opts.Strategy,
		overflow: opts.Overflow, restart: opts.Restart, capMin: opts.MinWorkers,
		capMax: opts.MaxWorkers, perWorker: opts.TargetPerWorker, maxRestart: opts.MaxRestarts,
		seed: seed, byWorker: map[WorkerID]*member{}, desired: opts.MinWorkers,
		rng: seed | 1,
	}
	if pl.strategy == ConsistentHash {
		pl.ring = newRing(seed, opts.RingReplicas)
	}
	p.pools[id] = pl
	p.mu.Unlock()
	p.fire(&PoolEvent{Kind: PoolCreated, Pool: id})

	// Create is atomic: either the full minimum is spawned, or the pool is torn
	// down and the error returned.
	spawned, err := p.grow(caller, id, opts.MinWorkers)
	if err != nil || spawned < opts.MinWorkers {
		_ = p.Destroy(id)
		if err == nil {
			err = ErrPoolWorkerLimit
		}
		return 0, err
	}
	return id, nil
}

// grow spawns up to n additional workers for the pool, respecting every ceiling
// (the pool's MaxWorkers and the service Limits). It returns the number spawned
// and the first error encountered.
func (p *Pools) grow(caller wago.HostModule, id PoolID, n uint32) (uint32, error) {
	var spawned uint32
	for i := uint32(0); i < n; i++ {
		p.mu.Lock()
		pl := p.pools[id]
		if pl == nil {
			p.mu.Unlock()
			return spawned, ErrPoolNotFound
		}
		if pl.destroyed || pl.draining {
			p.mu.Unlock()
			return spawned, ErrPoolDraining
		}
		live := uint32(len(pl.members))
		if live >= pl.capMax || live >= p.limits.MaxWorkersPerPool || p.total >= p.limits.MaxTotalWorkers {
			p.mu.Unlock()
			if spawned > 0 {
				return spawned, nil // reached a ceiling after making progress
			}
			return spawned, ErrPoolWorkerLimit
		}
		// Reserve the global slot before forking so concurrent grows cannot
		// overshoot MaxTotalWorkers; released below on any failure to add.
		p.total++
		wopts, tableIndex := pl.wopts, pl.tableIndex
		p.mu.Unlock()

		var wid WorkerID
		err := p.withWorkers(func(svc workers.Service) error {
			var err error
			wid, err = svc.Spawn(caller, tableIndex, wopts)
			if err != nil {
				return err
			}
			if err := svc.Link(caller, wid); err != nil {
				_ = svc.Kill(wid)
				return err
			}
			return nil
		})
		if err != nil {
			p.unreserve()
			return spawned, err
		}

		p.mu.Lock()
		pl = p.pools[id]
		if pl == nil || pl.destroyed {
			if p.total > 0 {
				p.total-- // release: the member is never added
			}
			p.mu.Unlock()
			_ = p.withWorkers(func(svc workers.Service) error { return svc.Kill(wid) })
			return spawned, ErrPoolNotFound
		}
		p.addMemberLocked(pl, wid)
		p.mu.Unlock()
		spawned++
		p.fire(&PoolEvent{Kind: PoolWorkerAdded, Pool: id, Worker: wid})
	}
	return spawned, nil
}

// addMemberLocked records a spawned worker. The global slot was already reserved
// by grow (p.total was incremented before the fork), so it is not incremented
// again here.
func (p *Pools) addMemberLocked(pl *pool, id WorkerID) {
	m := &member{id: id}
	pl.members = append(pl.members, m)
	pl.byWorker[id] = m
	if pl.ring != nil {
		pl.ring.add(id)
	}
	p.byWorker[id] = pl
}

// unreserve releases a global worker slot reserved by grow when the fork or the
// subsequent add did not complete.
func (p *Pools) unreserve() {
	p.mu.Lock()
	if p.total > 0 {
		p.total--
	}
	p.mu.Unlock()
}

func (p *Pools) removeMemberLocked(pl *pool, id WorkerID) {
	if _, ok := pl.byWorker[id]; !ok {
		return
	}
	delete(pl.byWorker, id)
	for i, m := range pl.members {
		if m.id == id {
			pl.members = append(pl.members[:i], pl.members[i+1:]...)
			break
		}
	}
	if pl.ring != nil {
		pl.ring.remove(id)
	}
	if n := uint32(len(pl.members)); n > 0 {
		pl.cursor %= n
	} else {
		pl.cursor = 0
	}
	delete(p.byWorker, id)
	if p.total > 0 {
		p.total--
	}
}

// Submit routes one task to a worker using the pool's strategy, keyed by tag.
// It returns the WorkerID that accepted the task. It does not need a caller and
// may be called from anywhere. See OverflowPolicy for full-mailbox behavior.
func (p *Pools) Submit(id PoolID, tag uint64, payload []byte) (WorkerID, error) {
	return p.submit(id, tag, tag, payload)
}

// SubmitKeyed is Submit with an explicit routing key, independent of the tag.
// The key drives ConsistentHash routing (and Random/PowerOfTwo tie-breaks); the
// tag is delivered to the worker unchanged.
func (p *Pools) SubmitKeyed(id PoolID, key, tag uint64, payload []byte) (WorkerID, error) {
	return p.submit(id, key, tag, payload)
}

// SubmitFrom reconciles the pool (respawning missing workers and applying
// autoscaling) using the live caller, then routes the task. Use it on a guest's
// hot path to make the pool self-heal and grow under load. It needs a caller.
func (p *Pools) SubmitFrom(caller wago.HostModule, id PoolID, tag uint64, payload []byte) (WorkerID, error) {
	if err := p.Reconcile(caller, id); err != nil {
		return 0, err
	}
	return p.submit(id, tag, tag, payload)
}

func (p *Pools) submit(id PoolID, key, tag uint64, payload []byte) (WorkerID, error) {
	if err := p.ensureWired(); err != nil {
		return 0, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, ErrPoolsClosed
	}
	pl := p.pools[id]
	if pl == nil {
		p.mu.Unlock()
		return 0, ErrPoolNotFound
	}
	if pl.draining || pl.destroyed {
		p.mu.Unlock()
		return 0, ErrPoolDraining
	}
	pl.submitted++
	if pl.strategy == Broadcast {
		ids := pl.snapshotIDs()
		p.mu.Unlock()
		var delivered int
		err := p.withWorkers(func(svc workers.Service) error {
			delivered = p.broadcastTo(id, svc, ids, tag, payload)
			return nil
		})
		if err != nil {
			return 0, err
		}
		_ = delivered
		return 0, nil
	}
	ordered := pl.order(key)
	var cands []WorkerID
	if pl.overflow == OverflowSpill {
		cands = make([]WorkerID, len(ordered))
		for i, m := range ordered {
			cands[i] = m.id
		}
	} else if len(ordered) > 0 {
		cands = []WorkerID{ordered[0].id}
	}
	overflow := pl.overflow
	p.mu.Unlock()

	if len(cands) == 0 {
		p.recordRejected(id)
		return 0, ErrPoolEmpty
	}
	var accepted WorkerID
	if err := p.withWorkers(func(svc workers.Service) error {
		for _, wid := range cands {
			if err := svc.Send(wid, tag, payload); err == nil {
				accepted = wid
				return nil
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if accepted != 0 {
		p.recordRouted(id, accepted)
		return accepted, nil
	}
	if overflow == OverflowShed {
		p.recordShed(id)
		return 0, nil
	}
	p.recordRejected(id)
	return 0, ErrPoolBackpressure
}

// order returns the pool's members in routing-preference order for the key.
// Called under p.mu; mutates the round-robin cursor and RNG state.
func (pl *pool) order(key uint64) []*member {
	n := len(pl.members)
	if n == 0 {
		return nil
	}
	un := uint32(n)
	switch pl.strategy {
	case RoundRobin:
		start := pl.cursor % un
		pl.cursor = (start + 1) % un
		return rotated(pl.members, start)
	case Random:
		return rotated(pl.members, uint32(pl.rand())%un)
	case LeastLoaded:
		out := append([]*member(nil), pl.members...)
		sort.SliceStable(out, func(i, j int) bool { return out[i].outstanding() < out[j].outstanding() })
		return out
	case PowerOfTwo:
		out := append([]*member(nil), pl.members...)
		if n >= 2 {
			a, b := uint32(pl.rand())%un, uint32(pl.rand())%un
			if out[b].outstanding() < out[a].outstanding() {
				a = b
			}
			out[0], out[a] = out[a], out[0]
		}
		return out
	case ConsistentHash:
		out := make([]*member, 0, n)
		if id, ok := pl.ring.get(key); ok {
			if m := pl.byWorker[id]; m != nil {
				out = append(out, m)
			}
		}
		rest := make([]*member, 0, n)
		for _, m := range pl.members {
			if len(out) == 0 || m.id != out[0].id {
				rest = append(rest, m)
			}
		}
		sort.SliceStable(rest, func(i, j int) bool { return rest[i].outstanding() < rest[j].outstanding() })
		return append(out, rest...)
	default:
		return append([]*member(nil), pl.members...)
	}
}

// rotated returns members starting at index start and wrapping around.
func rotated(members []*member, start uint32) []*member {
	n := uint32(len(members))
	out := make([]*member, 0, n)
	for i := uint32(0); i < n; i++ {
		out = append(out, members[(start+i)%n])
	}
	return out
}

// Broadcast delivers the task to every current worker in the pool and returns
// the number of workers that accepted it.
func (p *Pools) Broadcast(id PoolID, tag uint64, payload []byte) (int, error) {
	if err := p.ensureWired(); err != nil {
		return 0, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, ErrPoolsClosed
	}
	pl := p.pools[id]
	if pl == nil {
		p.mu.Unlock()
		return 0, ErrPoolNotFound
	}
	if pl.draining || pl.destroyed {
		p.mu.Unlock()
		return 0, ErrPoolDraining
	}
	pl.submitted++
	ids := pl.snapshotIDs()
	p.mu.Unlock()
	var delivered int
	err := p.withWorkers(func(svc workers.Service) error {
		delivered = p.broadcastTo(id, svc, ids, tag, payload)
		return nil
	})
	return delivered, err
}

func (p *Pools) broadcastTo(id PoolID, svc workers.Service, ids []WorkerID, tag uint64, payload []byte) int {
	delivered := 0
	for _, wid := range ids {
		if err := svc.Send(wid, tag, payload); err == nil {
			delivered++
			p.recordRouted(id, wid)
		} else {
			p.recordRejected(id)
		}
	}
	return delivered
}

func (p *Pools) recordRouted(id PoolID, wid WorkerID) {
	p.mu.Lock()
	if pl := p.pools[id]; pl != nil {
		pl.routed++
		if m := pl.byWorker[wid]; m != nil {
			m.enqueued++
		}
	}
	p.mu.Unlock()
	p.fire(&PoolEvent{Kind: PoolTaskRouted, Pool: id, Worker: wid})
}

func (p *Pools) recordRejected(id PoolID) {
	p.mu.Lock()
	if pl := p.pools[id]; pl != nil {
		pl.rejected++
	}
	p.mu.Unlock()
	p.fire(&PoolEvent{Kind: PoolTaskRejected, Pool: id})
}

func (p *Pools) recordShed(id PoolID) {
	p.mu.Lock()
	if pl := p.pools[id]; pl != nil {
		pl.shed++
	}
	p.mu.Unlock()
	p.fire(&PoolEvent{Kind: PoolTaskRejected, Pool: id})
}

// Reconcile converges the pool toward its target size using the live caller:
// it recomputes the autoscale target from current load, spawns workers to make
// up any deficit, and kills idle workers when the pool is over its target. It
// needs a caller because spawning forks the calling instance.
func (p *Pools) Reconcile(caller wago.HostModule, id PoolID) error {
	if err := p.ensureWired(); err != nil {
		return err
	}
	p.mu.Lock()
	pl := p.pools[id]
	if pl == nil || pl.destroyed {
		p.mu.Unlock()
		return ErrPoolNotFound
	}
	pl.recomputeDesired()
	p.clampToRunnableBudgetLocked(pl)
	live := uint32(len(pl.members))
	desired := pl.desired
	var toKill []WorkerID
	if !pl.draining && live > desired {
		excess := live - desired
		for _, m := range pl.members {
			if excess == 0 {
				break
			}
			if m.outstanding() == 0 { // only reclaim idle workers, never drop queued work
				toKill = append(toKill, m.id)
				excess--
			}
		}
	}
	var deficit uint32
	if !pl.draining && desired > live {
		deficit = desired - live
	}
	p.mu.Unlock()

	if len(toKill) != 0 {
		if err := p.withWorkers(func(svc workers.Service) error {
			for _, wid := range toKill {
				_ = svc.Kill(wid)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if deficit > 0 {
		if _, err := p.grow(caller, id, deficit); err != nil {
			return err
		}
	}
	return nil
}

// recomputeDesired updates pl.desired. For an elastic pool it derives the target
// from current load, clamped to [MinWorkers, MaxWorkers]. For a fixed pool the
// target is managed by exits and Scale, so it is left unchanged (only lowered if
// crash-loop protection has halted replacement). Called under p.mu.
func (pl *pool) recomputeDesired() {
	if pl.halted {
		if n := uint32(len(pl.members)); n < pl.desired {
			pl.desired = n
		}
		return
	}
	if !pl.autoscale() {
		return
	}
	per := uint64(pl.perWorker)
	target := (pl.totalOutstanding() + per - 1) / per
	if target < uint64(pl.capMin) {
		target = uint64(pl.capMin)
	}
	if target > uint64(pl.capMax) {
		target = uint64(pl.capMax)
	}
	pl.desired = uint32(target)
}

// clampToRunnableBudgetLocked lowers an elastic pool's autoscale target so the
// total workers across all pools do not grow past the service's RunnableWorkers
// budget (the machine's parallelism by default), preventing CPU oversubscription
// when several elastic pools are busy at once. The pool's MinWorkers floor is
// always preserved — the budget caps growth, it never starves a pool below its
// minimum. Called under p.mu, after recomputeDesired.
func (p *Pools) clampToRunnableBudgetLocked(pl *pool) {
	if !pl.autoscale() {
		return
	}
	other := p.total - uint32(len(pl.members)) // workers held by the other pools
	room := pl.capMin
	if p.limits.RunnableWorkers > other {
		if avail := p.limits.RunnableWorkers - other; avail > room {
			room = avail
		}
	}
	if pl.desired > room {
		pl.desired = room
	}
}

// Scale sets a fixed-size pool's target worker count (clamped to the pool's
// MaxWorkers and the service Limits) and reconciles to it. On an elastic pool the
// target is advisory — the next reconcile re-derives it from load.
func (p *Pools) Scale(caller wago.HostModule, id PoolID, target uint32) error {
	if err := p.ensureWired(); err != nil {
		return err
	}
	p.mu.Lock()
	pl := p.pools[id]
	if pl == nil || pl.destroyed {
		p.mu.Unlock()
		return ErrPoolNotFound
	}
	if pl.draining {
		p.mu.Unlock()
		return ErrPoolDraining
	}
	max := pl.capMax
	if p.limits.MaxWorkersPerPool < max {
		max = p.limits.MaxWorkersPerPool
	}
	if target > max {
		target = max
	}
	pl.desired = target
	p.mu.Unlock()
	p.fire(&PoolEvent{Kind: PoolScaled, Pool: id})
	return p.Reconcile(caller, id)
}

// Drain stops the pool from accepting new tasks and tears it down gracefully:
// idle workers are killed immediately, and busy workers are killed as soon as
// their mailbox empties. It does not need a caller.
func (p *Pools) Drain(id PoolID) error {
	if err := p.ensureWired(); err != nil {
		return err
	}
	p.mu.Lock()
	pl := p.pools[id]
	if pl == nil || pl.destroyed {
		p.mu.Unlock()
		return ErrPoolNotFound
	}
	pl.draining = true
	pl.desired = 0
	var killNow []WorkerID
	for _, m := range pl.members {
		if m.outstanding() == 0 {
			killNow = append(killNow, m.id)
		} else {
			m.killWhenIdle = true
		}
	}
	p.mu.Unlock()
	if err := p.withWorkers(func(svc workers.Service) error {
		for _, wid := range killNow {
			_ = svc.Kill(wid)
		}
		return nil
	}); err != nil {
		return err
	}
	p.fire(&PoolEvent{Kind: PoolDrained, Pool: id})
	return nil
}

// Destroy removes the pool and kills all of its workers immediately, dropping
// any queued tasks. It does not need a caller.
func (p *Pools) Destroy(id PoolID) error {
	if err := p.ensureWired(); err != nil {
		return err
	}
	p.mu.Lock()
	pl := p.pools[id]
	if pl == nil {
		p.mu.Unlock()
		return ErrPoolNotFound
	}
	pl.destroyed = true
	ids := pl.snapshotIDs()
	delete(p.pools, id)
	for _, wid := range ids {
		delete(p.byWorker, wid)
		if p.total > 0 {
			p.total--
		}
	}
	p.mu.Unlock()
	if err := p.withWorkers(func(svc workers.Service) error {
		for _, wid := range ids {
			_ = svc.Kill(wid)
		}
		return nil
	}); err != nil {
		return err
	}
	p.fire(&PoolEvent{Kind: PoolDestroyed, Pool: id})
	return nil
}

// Stats returns an atomic snapshot of the pool.
func (p *Pools) Stats(id PoolID) (PoolStats, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pl := p.pools[id]
	if pl == nil {
		return PoolStats{}, ErrPoolNotFound
	}
	st := PoolStats{
		Pool: id, Strategy: pl.strategy, Live: uint32(len(pl.members)), Desired: pl.desired,
		Min: pl.capMin, Max: pl.capMax, Draining: pl.draining, Halted: pl.halted,
		Submitted: pl.submitted, Routed: pl.routed, Rejected: pl.rejected, Shed: pl.shed,
		Restarted: pl.restarts, Exited: pl.exited, Failed: pl.failed,
	}
	for _, m := range pl.members {
		o := m.outstanding()
		st.Outstanding += uint64(o)
		st.Workers = append(st.Workers, WorkerStat{ID: m.id, Enqueued: m.enqueued, Dispatched: m.dispatched, Outstanding: o})
	}
	return st, nil
}

// List returns the IDs of all live pools, in ascending order.
func (p *Pools) List() []PoolID {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]PoolID, 0, len(p.pools))
	for id := range p.pools {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// onMessage runs on a worker goroutine when a message is delivered. It advances
// the member's dispatched count and completes a graceful drain when the worker's
// mailbox empties.
func (p *Pools) onMessage(ctx *workers.MessageContext) {
	p.mu.Lock()
	pl := p.byWorker[ctx.WorkerID]
	var kill bool
	if pl != nil {
		if m := pl.byWorker[ctx.WorkerID]; m != nil {
			m.dispatched++
			if m.killWhenIdle && m.outstanding() == 0 {
				kill = true
			}
		}
	}
	p.mu.Unlock()
	if kill {
		_ = p.withWorkers(func(svc workers.Service) error { return svc.Kill(ctx.WorkerID) })
	}
}

// onExit runs on a worker goroutine when a worker exits. It removes the member,
// updates counters, and applies the restart policy.
func (p *Pools) onExit(ctx *workers.WorkerExitContext) {
	p.mu.Lock()
	pl := p.byWorker[ctx.WorkerID]
	if pl == nil {
		p.mu.Unlock()
		return
	}
	p.removeMemberLocked(pl, ctx.WorkerID)
	pl.exited++
	if ctx.Kind == workers.WorkerFailed {
		pl.failed++
	}
	p.applySupervisionLocked(pl, ctx.Kind)
	p.mu.Unlock()
	p.fire(&PoolEvent{Kind: PoolWorkerRemoved, Pool: pl.id, Worker: ctx.WorkerID, ExitKind: ctx.Kind, Err: ctx.Err})
}

// applySupervisionLocked adjusts the pool's target after a worker exit. Called
// under p.mu, after the member has been removed.
func (p *Pools) applySupervisionLocked(pl *pool, kind workers.WorkerExitKind) {
	if pl.draining || pl.destroyed {
		return
	}
	if pl.autoscale() {
		// Elastic pools self-heal to the load-derived target on the next reconcile;
		// only failures count against crash-loop protection.
		if kind == workers.WorkerFailed {
			pl.restarts++
			if pl.maxRestart != UnlimitedRestarts && pl.restarts >= uint64(pl.maxRestart) {
				pl.halted = true
			}
		}
		return
	}
	replace := false
	switch pl.restart {
	case RestartAlways:
		replace = true
	case RestartOnFailure:
		replace = kind != workers.WorkerReturned
	case RestartNever:
		replace = false
	}
	if pl.halted {
		replace = false
	}
	if replace && pl.maxRestart != UnlimitedRestarts && pl.restarts >= uint64(pl.maxRestart) {
		replace = false
		pl.halted = true
	}
	if replace {
		pl.restarts++ // desired unchanged; live < desired now, so reconcile refills
	} else if pl.desired > 0 {
		pl.desired--
	}
}

func (p *Pools) close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	p.wireMu.Lock()
	message, exit := p.message, p.exit
	p.message, p.exit = workers.Subscription{}, workers.Subscription{}
	p.wired = false
	p.wireMu.Unlock()
	var unsubscribeErr error
	if p.ref != nil {
		unsubscribeErr = p.ref.With(func(service workers.Service) error {
			return errors.Join(service.Unsubscribe(message), service.Unsubscribe(exit))
		})
	}
	p.obsMu.Lock()
	observers := make([]*eventObserver, 0, len(p.obs))
	for _, observer := range p.obs {
		observers = append(observers, observer)
	}
	p.obs = nil
	p.hasObs.Store(false)
	panicErrs := append([]error(nil), p.observerPanics...)
	p.observerPanics = nil
	p.obsMu.Unlock()
	for _, observer := range observers {
		observer.stop()
	}
	p.mu.Lock()
	p.pools = map[PoolID]*pool{}
	p.byWorker = map[WorkerID]*pool{}
	p.mu.Unlock()
	return errors.Join(append([]error{unsubscribeErr}, panicErrs...)...)
}

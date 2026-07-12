<div align="center">
    <h1><code>pool</code></h1>
    <p>Elastic, load-balanced, self-healing pools of workers for the <a href="https://github.com/wago-org/wago">Wago</a> WebAssembly runtime.</p>
</div>

<p align="center">
    <a href="https://github.com/JairusSW/pool/actions/workflows/ci.yml"><img src="https://github.com/JairusSW/pool/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://codecov.io/gh/JairusSW/pool"><img src="https://codecov.io/gh/JairusSW/pool/branch/main/graph/badge.svg" alt="Coverage"></a>
    <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.24-00ADD8.svg" alt="Go >= 1.24"></a>
    <a href="https://github.com/wago-org/wago"><img src="https://img.shields.io/badge/wago-%3E%3D0.1.0-6E56CF.svg" alt="Wago >= 0.1.0"></a>
</p>

<details>
<summary>Table of Contents</summary>

- [Overview](#overview)
- [Installation](#installation)
- [Concepts](#concepts)
- [Usage](#usage)
- [API](#api)
  - [Creating a pool](#creating-a-pool)
  - [Submitting work](#submitting-work)
  - [Routing strategies](#routing-strategies)
  - [Overflow policies](#overflow-policies)
  - [Autoscaling](#autoscaling)
  - [Supervision & restart](#supervision--restart)
  - [Draining & teardown](#draining--teardown)
  - [Metrics & events](#metrics--events)
  - [Options & limits](#options--limits)
  - [Errors](#errors)
- [How it composes](#how-it-composes)
- [Testing](#testing)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

</details>

## Overview

`pool` is an optional [Wago](https://github.com/wago-org/wago) plugin that turns the
low-level worker primitives of [`workers`](https://github.com/wago-org/workers) into
**managed pools**: named groups of workers that all run the same guest table entry,
with a routing strategy in front, supervision behind, and elastic sizing on top.

It is the policy layer `workers` deliberately leaves out. Where `workers` gives you
`Spawn`, `Send`, `DispatchNext`, `Link`, and `Kill`, `pool` gives you:

- **Load balancing** across a fleet with six routing strategies — round-robin,
  least-loaded, random, power-of-two-choices, consistent-hash, and broadcast.
- **Autoscaling** — a pool grows under load and shrinks back when idle, driven by a
  simple, timer-free target: outstanding tasks per worker. The ceiling is computed
  from the machine's parallelism by default, so pools fill the CPU without
  oversubscribing it.
- **Supervision** — workers that trap, are killed, or return are replaced according
  to a restart policy, with crash-loop protection.
- **Backpressure** — full mailboxes spill to other workers, reject, or shed, your
  choice.
- **Graceful draining** — stop accepting work and let in-flight tasks finish before
  workers are torn down.
- **Observability** — per-pool and per-worker counters, plus a stream of lifecycle
  and routing events.

The whole thing is bounded by construction: every pool has a worker ceiling, and the
service has aggregate caps on pools and total workers — on top of the hard ceiling
enforced by the underlying `workers` service.

> **Stability:** experimental (`v0.1.0`). The API may change before `v1.0.0`.

## Installation

If you have the [`wago`](https://github.com/wago-org/wago) CLI installed:

```sh
wago pkg install github.com/JairusSW/pool
```

or use [`go get`](https://pkg.go.dev/cmd/go#hdr-Get_packages_and_dependencies):

```sh
go get github.com/JairusSW/pool
```

The pool plugin needs **no privileged capabilities of its own** — it composes the
`workers` service, which holds the privileged instance capabilities. A host that uses
pool must therefore also register `workers` and grant it `instance.manage` and
`instance.lifecycle`:

```json
{
  "dependencies": ["github.com/JairusSW/pool", "github.com/wago-org/workers"],
  "plugins": [
    {
      "name": "github.com/wago-org/workers",
      "capabilities": {
        "instance.manage": { "maxInstances": 256 },
        "instance.lifecycle": true
      }
    },
    { "name": "github.com/JairusSW/pool" }
  ]
}
```

## Concepts

| Term | Meaning |
| --- | --- |
| **Plugin** | `pool.New()` — the `wago.Extension` you register with a runtime. It provides one **service**. |
| **Service** (`*pool.Pools`) | The host-side handle you use to create pools and route work. Obtain it with `Plugin.Service()` or through `pool.ServiceKey`. |
| **Pool** | A named group of workers (`PoolID`) that all run the same table entry, fronted by a routing strategy and kept at a target size. |
| **Worker** | One member of a pool — a `workers` worker (`WorkerID`) driven on its own goroutine. |
| **Task** | A tagged byte payload routed to one worker (or broadcast to all). "Distribute stuff" = submit tasks. |
| **Desired size** | The number of workers a pool is converging toward — set by `MinWorkers`, autoscaling, `Scale`, or supervision. |

A pool's life: **create → route tasks across members → grow/shrink & self-heal via
`Reconcile` → drain → destroy.**

## Usage

Register `workers`, register `pool` wired to it, and drive it from your own host
imports — the embedder owns the guest ABI and passes the live caller through:

```go
package main

import (
	"log"

	"github.com/wago-org/wago"
	"github.com/wago-org/workers"
	"github.com/JairusSW/pool"
)

func main() {
	rt := wago.NewRuntime()
	defer rt.Close()

	// 1. The workers service holds the privileged instance capabilities.
	wk := workers.New(workers.WithLimits(workers.WorkerLimits{MaxLiveWorkers: 256}))
	if err := rt.Use(wk, wago.WithPluginGrants(
		wago.PluginManagedInstances, // "instance.manage"
		wago.PluginInstanceHooks,    // "instance.lifecycle"
	)); err != nil {
		log.Fatal(err)
	}

	// 2. The pool service composes it. For programmatic embedding via Use, wire it
	//    explicitly with WithWorkers; under the manifest/LoadPlugins plan path the
	//    runtime binds it automatically through workers.ServiceKey.
	pl := pool.New(pool.WithWorkers(wk.Service()))
	if err := rt.Use(pl); err != nil {
		log.Fatal(err)
	}
	pools := pl.Service()

	// 3. Observe pool lifecycle and routing (optional).
	pools.OnEvent(func(ev *pool.PoolEvent) {
		log.Printf("pool %d event kind=%d worker=%d", ev.Pool, ev.Kind, ev.Worker)
	})

	// ... inside a host import, create a pool from the calling instance and route:
	//     id, _ := pools.Create(caller, tableIndex, pool.PoolOptions{
	//         Strategy: pool.LeastLoaded, MinWorkers: 4, MaxWorkers: 32, TargetPerWorker: 8,
	//     })
	//     pools.Submit(id, tag, payload)   // no caller needed
}
```

`Create`, `Reconcile`, `Scale`, and `SubmitFrom` fork the calling instance, so they
must run **inside a live host import**, where `caller` is the active
`wago.HostModule`. `Submit`, `SubmitKeyed`, `Broadcast`, `Drain`, `Destroy`, and
`Stats` take no caller and may be called from anywhere.

## API

### Creating a pool

```go
id, err := pools.Create(caller, tableIndex, pool.PoolOptions{
	Strategy:        pool.LeastLoaded,
	MinWorkers:      4,
	MaxWorkers:      32,
	TargetPerWorker: 8,       // autoscale: ~8 outstanding tasks per worker
	Restart:         pool.RestartOnFailure,
})
```

`Create` spawns `MinWorkers` workers immediately, each running the guest table entry
at `tableIndex` (which must have the Wasm signature `() -> ()`), forked from the
calling instance and linked to it so the pool is torn down when the caller closes. If
it cannot spawn the full minimum, the partial pool is destroyed and the error is
returned.

### Submitting work

`Submit` routes one task to a worker using the pool's strategy and returns the
`WorkerID` that accepted it. It copies the payload and never blocks:

```go
wid, err := pools.Submit(id, tag, payload)
```

- `SubmitKeyed(id, key, tag, payload)` routes by an explicit key (independent of the
  tag) — this is the routing key for `ConsistentHash`.
- `SubmitFrom(caller, id, tag, payload)` reconciles the pool first (self-healing and
  autoscaling from a live caller), then routes. Use it on a guest's hot path to make
  the pool grow and recover under load.
- `Broadcast(id, tag, payload)` delivers the task to every worker and returns the
  count that accepted it.

### Routing strategies

| Strategy | Behavior |
| --- | --- |
| `RoundRobin` | Cycles through workers in order — even spread. **(default)** |
| `LeastLoaded` | Routes to the worker with the smallest mailbox backlog. |
| `Random` | Uniformly random worker. |
| `PowerOfTwo` | Samples two workers, picks the less loaded — near-least-loaded at O(1). |
| `ConsistentHash` | Routes by key via a virtual-node hash ring: same key → same worker, and scaling remaps only a fraction of keys. |
| `Broadcast` | Every task goes to every worker. |

### Overflow policies

When a routed task's chosen worker mailbox is full, `OverflowPolicy` decides what
happens:

| Policy | Behavior |
| --- | --- |
| `OverflowSpill` | Try the remaining workers in preference order. **(default)** |
| `OverflowReject` | Fail fast with `ErrPoolBackpressure`. |
| `OverflowShed` | Drop the task silently (counted as *shed*), returning `(0, nil)`. |

### Autoscaling

Set `TargetPerWorker > 0` to make a pool **elastic**. Its desired size is derived —
with no timers — from current load:

```
desired = clamp(ceil(totalOutstanding / TargetPerWorker), MinWorkers, MaxWorkers)
```

`Reconcile(caller, id)` converges the pool to that target: it spawns workers to make
up a deficit (needs the live caller, since spawning forks it) and kills **idle**
workers when the pool is over target — never dropping queued work. `SubmitFrom` calls
`Reconcile` for you on the hot path, so a busy pool grows itself.

**Optimal sizing, no oversubscription.** You don't have to guess a ceiling. Leave
`MaxWorkers` at `0` on an elastic pool and it is sized to `OptimalWorkers()` — the
machine's parallelism (`GOMAXPROCS`) — so the pool grows to fill the CPU but never
runs more compute-bound workers than the hardware can execute in parallel (which only
adds context-switching, scheduler contention, and cache pressure). Across pools, the
service-wide `RunnableWorkers` budget (also `OptimalWorkers()` by default) caps how
many workers autoscaling will run at once, so several busy elastic pools can't
collectively oversubscribe the CPU. Each pool's `MinWorkers` floor is always honored;
the budget caps *growth*, never starves a pool. Call `pool.OptimalWorkers()` yourself
to size things explicitly, and set `MaxWorkers`/`RunnableWorkers` higher for
I/O-bound workloads that benefit from more concurrency than cores.

For a **fixed** pool (leave `TargetPerWorker` at 0), the size is what you set:
`MinWorkers` at creation, adjusted by `Scale(caller, id, target)` and supervision.

### Supervision & restart

When a worker exits, the pool reacts according to `RestartPolicy` (fixed pools) or
self-heals to the load target (elastic pools):

| Policy | Behavior |
| --- | --- |
| `RestartOnFailure` | Replace a worker that trapped or was killed; let a clean return shrink the pool. **(default)** |
| `RestartAlways` | Replace every exited worker, keeping the pool full even as workers return. |
| `RestartNever` | Never replace; the pool shrinks permanently. |

Replacement happens on the next `Reconcile`/`SubmitFrom` (which supplies a live
caller). `MaxRestarts` caps total replacements over a pool's life; exceeding it
**halts** replacement (crash-loop protection) and is reported in `Stats().Halted`. Use
`UnlimitedRestarts` to disable the cap.

### Draining & teardown

```go
pools.Drain(id)    // stop accepting; kill idle workers now, busy ones once they drain
pools.Destroy(id)  // kill everything immediately and remove the pool
```

`Drain` is graceful: new submits are rejected with `ErrPoolDraining`, workers with an
empty mailbox are killed at once, and each remaining worker is killed the moment its
mailbox empties. `Destroy` is immediate and drops any queued tasks.

### Metrics & events

`Stats(id)` returns an atomic snapshot — pool size and target, per-worker backlog, and
lifetime counters (submitted, routed, rejected, shed, restarted, exited, failed):

```go
st, _ := pools.Stats(id)
log.Printf("live=%d desired=%d routed=%d rejected=%d outstanding=%d",
	st.Live, st.Desired, st.Routed, st.Rejected, st.Outstanding)
```

`OnEvent` streams `PoolEvent`s — `PoolCreated`, `PoolWorkerAdded`, `PoolWorkerRemoved`
(with the exit kind), `PoolScaled`, `PoolTaskRouted`, `PoolTaskRejected`,
`PoolDrained`, `PoolDestroyed` — for logging, metrics, or external orchestration.
Observers add zero cost to the hot path when none are registered.

### Options & limits

`PoolOptions` (per pool):

| Field | Default | Meaning |
| --- | --- | --- |
| `Strategy` | `RoundRobin` | Routing algorithm. |
| `Overflow` | `OverflowSpill` | Full-mailbox behavior. |
| `Restart` | `RestartOnFailure` | Replacement policy for a fixed pool. |
| `MinWorkers` | `1` | Initial size and autoscale floor. |
| `MaxWorkers` | `OptimalWorkers()` if elastic, else `= MinWorkers` | Autoscale ceiling. `0` auto-sizes an elastic pool to the CPU. |
| `TargetPerWorker` | `0` (off) | Outstanding tasks per worker; turns autoscaling on. |
| `MaxRestarts` | `16` | Replacement budget; `UnlimitedRestarts` disables it. |
| `Worker` | `workers` defaults | Per-worker mailbox bounds (`workers.WorkerOptions`). |
| `HashSeed` | derived from `PoolID` | Seed for the ring and random strategies. |
| `RingReplicas` | `64` | Virtual nodes per worker on the `ConsistentHash` ring. |

`Limits` (whole service, via `pool.WithLimits`):

| Field | Default | Meaning |
| --- | --- | --- |
| `MaxPools` | `64` | Maximum pools at once. |
| `MaxWorkersPerPool` | `64` | Cap per pool, overriding a larger `MaxWorkers`. |
| `MaxTotalWorkers` | `256` | Hard cap on workers summed across all pools. |
| `RunnableWorkers` | `OptimalWorkers()` | Cap on workers autoscaling will run across all pools, to avoid CPU oversubscription. Growth-only; floors are always honored. |

`MaxPools`/`MaxWorkersPerPool`/`MaxTotalWorkers` are hard safety bounds;
`RunnableWorkers` is the performance guard that keeps elastic pools from
oversubscribing the CPU. All of these sit above the `workers` service's own
`WorkerLimits`, which is the hard ceiling on live workers.

### Errors

All errors are comparable sentinels — match them with `errors.Is`.

| Error | When |
| --- | --- |
| `ErrPoolsInactive` | Service is nil or not wired to a workers service. |
| `ErrPoolsClosed` | The service has been stopped. |
| `ErrWorkersUnavailable` | The workers service could not be resolved. |
| `ErrPoolNotFound` | No pool with that ID. |
| `ErrPoolEmpty` | The pool has no live workers to route to. |
| `ErrPoolDraining` | The pool is draining or destroyed. |
| `ErrPoolBackpressure` | Every candidate worker was at capacity. |
| `ErrInvalidPoolOptions` | Options are inconsistent (e.g. `MaxWorkers < MinWorkers`). |
| `ErrPoolLimit` | The service's `MaxPools` was reached. |
| `ErrPoolWorkerLimit` | A per-pool or total worker ceiling was reached. |
| `ErrPoolIDExhausted` | The `PoolID` space is exhausted. |

## How it composes

`pool` → `workers` → `wago`. The pool service never touches the core instance API
directly; it holds a reference to the `workers` service and expresses everything —
spawning, routing, teardown, supervision — through `Spawn`, `Send`, `DispatchNext`,
`Link`, `Kill`, and the `OnMessage`/`OnExit` observers. This is the plugin-composition
story Wago is built for: one plugin's service becomes another's foundation.

Two wiring paths are supported:

- **Manifest / `LoadPlugins`** — the runtime resolves `workers.ServiceKey` and binds it
  into the pool plugin automatically. This is the production path.
- **Programmatic `Runtime.Use`** — pass the workers handle explicitly with
  `pool.WithWorkers(wk.Service())`, since `Use` commits each plugin independently and
  does not run cross-plugin service resolution.

A generated Wago host does not import this package directly; it blank-imports the
[`register`](./register) subpackage, which activates init-time registration for both
pool and its `workers` dependency:

```go
import _ "github.com/JairusSW/pool/register"
```

## Testing

```sh
go test ./...
go test -race ./...   # the concurrency paths are the whole point
go test -short ./...  # smaller stress sizes for a quick pass
```

The suite mixes pure-logic unit tests (strategies, the autoscale target, supervision
state machine, the hash ring) with end-to-end integration tests that build real guest
modules via `github.com/wago-org/wago/testutil/wasmtest` — spawning pools, load
balancing across them, poisoning workers and healing, draining, and autoscaling from a
real backlog — plus concurrency stress tests that hammer pools from many producers
while workers churn.

## Architecture

- **`pool.go`** — the whole service: the `Plugin` extension, the `Pools` service,
  routing, overflow, autoscaling, supervision, draining, and the `OnMessage`/`OnExit`
  wiring onto the workers service.
- **`ring.go`** — the virtual-node consistent-hash ring for `ConsistentHash` routing.
- **`register/`** — a blank-import shim that activates registration for pool and its
  workers dependency.
- **`wago.json`** — the package manifest.

Design notes:

- **One lock, short critical sections.** All pool state is guarded by a single mutex;
  the mutex is never held across a `workers` call or an observer callback, so routing
  scales and there is no lock-order cycle with the worker goroutines that deliver
  `OnMessage`/`OnExit`.
- **Timer-free autoscaling.** Desired size is a pure function of current load, applied
  on `Reconcile`. There are no background goroutines to reason about or shut down.
- **Caller-safe healing.** Spawning forks the calling instance, so it only happens
  inside a live host import. Supervision records a deficit; the next
  `Reconcile`/`SubmitFrom` fills it with the live caller.

## Contributing

Contributions are welcome! Please:

- Run `go test -race ./...` and `go vet ./...` before opening a pull request.
- Keep the layering intact — `pool` builds on the `workers` service and adds policy;
  it does not reach into the core instance API.
- Follow standard Go formatting (`gofmt`) and conventional commit messages.

## License

This project is distributed under the [Apache License 2.0](./LICENSE). Work on this
project is done out of passion — if you want to support it financially, you can donate
through [GitHub Sponsors](https://github.com/sponsors/JairusSW).

## Contact

Please file issues at [GitHub Issues](https://github.com/JairusSW/pool/issues). To chat,
join the [Wago Discord](https://wago.sh/discord).

- **GitHub:** [https://github.com/wago-org/](https://github.com/wago-org/)
- **Website:** [https://wago.sh/](https://wago.sh/)
- **Discord:** [https://wago.sh/discord](https://wago.sh/discord)

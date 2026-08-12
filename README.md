<div align="center">
  <h1><code>pool</code></h1>
  <p>Elastic, load-balanced, self-healing pools of Wago workers.</p>
</div>

Pool is the policy layer above
[github.com/wago-org/workers](https://github.com/wago-org/workers). It groups
workers that run the same guest table entry, routes tagged byte payloads across
them, reconciles target size, and applies bounded restart and autoscaling rules.

Workers owns the privileged instances. Pool has no Wago authorities of its own;
it depends on Workers and calls its typed contract.

> Pool is experimental (`v0.1.0`). Its API may change before the first stable
> release.

## Install

```sh
wago add github.com/JairusSW/pool
```

The resolver adds `github.com/wago-org/workers`, reviews its authorities, and
locks both the package dependency and Pool's exact
`github.com/wago-org/workers/service@1` binding. The generated runtime links
both explicit `/register` catalogs; neither package self-registers from `init`.

## Use from a plugin

Pool provides `github.com/JairusSW/pool/service@1`:

```go
func Definition() wago.PluginDefinition {
	return wago.PluginDefinition{
		// ...ID, version, and provenance...
		Requires: []wago.PluginRequirement{{
			ID: pool.PluginID, Version: "^0.1.0",
		}},
		Consumes: []wago.ContractRequirement{{
			ID: pool.Contract.ID(), Major: pool.Contract.Major(),
			Mode: wago.ContractRequired,
		}},
	}
}

poolRef, err := wagoplugin.Require(reg, pool.Contract)
if err != nil {
	return err
}

err = poolRef.With(func(service pool.Service) error {
	id, err := service.Create(caller, tableIndex, pool.PoolOptions{
		Strategy:   pool.PowerOfTwo,
		MinWorkers: 2,
		MaxWorkers: 16,
	})
	if err != nil {
		return err
	}
	_, err = service.Submit(id, 42, payload)
	return err
})
```

The package edge installs Pool and its Workers dependency transitively. The
contract edge pins the exact reviewed Pool provider the consumer may call.

`Create`, `Reconcile`, `Scale`, and `SubmitFrom` require the active
`wago.HostModule`, because growing a pool forks the exact guest caller.
`Submit`, `SubmitKeyed`, `Broadcast`, `Drain`, `Destroy`, `Stats`, and `List`
can be called without a guest caller.

The service is available only inside `Ref.With`. Wago prevents new calls after
the consumer stops and waits for calls already in flight before Workers stops.

## Routing and supervision

Strategies:

- `RoundRobin`
- `LeastLoaded`
- `Random`
- `PowerOfTwo`
- `ConsistentHash`
- `Broadcast`

Overflow can spill to another worker, reject immediately, or shed a task.
Restart policy can replace failed workers, every exit, or no exits. A bounded
restart counter stops crash loops. Autoscaling derives a target from outstanding
tasks and clamps it to the pool and service ceilings; the default runnable
ceiling is `GOMAXPROCS`.

Pool events use owned subscriptions:

```go
events, err := service.ObserveEvents(func(event *pool.PoolEvent) {
	log.Printf("pool %d event %d", event.Pool, event.Kind)
})
// In the consuming plugin's Stop callback, through poolRef.With:
err = service.UnsubscribeEvents(events)
```

Unsubscribing waits for callbacks already in flight. It must not be called
inside its own callback.

## Configure

```sh
wago plugin config github.com/JairusSW/pool \
  '{"maxPools":64,"maxWorkersPerPool":64,"maxTotalWorkers":256,"runnableWorkers":16}'
```

All four limits must be positive when explicitly set. Configuration is strict:
unknown fields, trailing JSON, and a per-pool ceiling above the total ceiling
are rejected before activation.

## Test

```sh
go test ./...
go test -race ./...
```

The suite exercises the real Workers → Pool → consumer contract graph, routing
strategies, overflow, supervision, autoscaling, teardown, and concurrent stress.

## License

Apache-2.0. See [LICENSE](./LICENSE).

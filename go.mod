module github.com/JairusSW/pool

go 1.24.0

require (
	github.com/wago-org/wago v0.1.0
	github.com/wago-org/workers v0.1.0
)

// The wago engine and the workers plugin are not yet published with version tags,
// so build against the sibling checkouts. Remove these replaces once the modules
// have tagged releases and bump the requires above to them.
replace (
	github.com/wago-org/wago => ../wago
	github.com/wago-org/workers => ../workers
)

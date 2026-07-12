// Package register activates the pool plugin's init-time registration for
// Wago-generated hosts. Applications normally import github.com/JairusSW/pool
// directly; generated hosts blank-import this package.
//
//	import _ "github.com/JairusSW/pool/register"
//
// The pool plugin builds on the workers plugin, so a host that registers pool
// must also register workers. Blank-importing workers/register here guarantees
// the dependency is present in the build; the runtime enforces load ordering.
package register

import (
	_ "github.com/JairusSW/pool"
	_ "github.com/wago-org/workers/register"
)

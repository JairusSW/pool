// Package register exposes Pool's explicit provider catalog to generated Wago
// runtimes. Importing this package has no registration side effects.
package register

import (
	"github.com/JairusSW/pool"
	"github.com/wago-org/wago"
)

func Providers() []wago.PluginProvider {
	return []wago.PluginProvider{pool.Provider()}
}

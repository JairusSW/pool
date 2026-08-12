package register

import (
	"testing"

	"github.com/JairusSW/pool"
)

func TestProvidersIsExplicitAndFresh(t *testing.T) {
	first, second := Providers(), Providers()
	if len(first) != 1 || first[0].Definition.ID != pool.PluginID {
		t.Fatalf("Providers() = %#v", first)
	}
	first[0].Definition.ID = "example.com/mutated"
	if second[0].Definition.ID != pool.PluginID {
		t.Fatal("provider catalog shares mutable definition state")
	}
}

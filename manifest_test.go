package pool_test

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/JairusSW/pool"
	poolregister "github.com/JairusSW/pool/register"
	"github.com/wago-org/wago"
	"github.com/wago-org/workers"
)

type manifestAuthor struct {
	Name string `json:"name"`
}

type manifestPackage struct {
	Module      string            `json:"module"`
	Version     string            `json:"version"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Stability   wago.Stability    `json:"stability"`
	License     string            `json:"license"`
	Homepage    string            `json:"homepage"`
	Repository  string            `json:"repository"`
	Authors     []manifestAuthor  `json:"authors"`
	Engines     map[string]string `json:"engines"`
	Platforms   []string          `json:"platforms"`
}

func TestManifestMatchesCatalogMetadataAndWorkersDependency(t *testing.T) {
	raw, err := os.ReadFile("wago.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Schema  string            `json:"$schema"`
		Package manifestPackage   `json:"package"`
		Plugins map[string]string `json:"plugins"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Schema != "https://wago.sh/v1/schema.json" {
		t.Fatalf("manifest schema = %q", manifest.Schema)
	}
	providers := poolregister.Providers()
	if len(providers) != 1 {
		t.Fatalf("catalog providers = %d, want 1", len(providers))
	}
	definition := pool.Definition()
	if !reflect.DeepEqual(providers[0].Definition, definition) {
		t.Fatalf("catalog definition drifted\ncatalog=%#v\ncanonical=%#v", providers[0].Definition, definition)
	}
	assertManifestMetadata(t, manifest.Package, definition)
	if len(definition.Requires) != 1 || definition.Requires[0].ID != workers.PluginID {
		t.Fatalf("Workers definition requirement = %#v", definition.Requires)
	}
	if got := manifest.Plugins[workers.PluginID]; len(manifest.Plugins) != 1 || got != definition.Requires[0].Version {
		t.Fatalf("Workers manifest dependency = %v, definition = %#v", manifest.Plugins, definition.Requires)
	}
	if len(definition.Consumes) != 1 || definition.Consumes[0].ID != workers.Contract.ID() ||
		definition.Consumes[0].Major != workers.Contract.Major() || definition.Consumes[0].Mode != wago.ContractRequired {
		t.Fatalf("Workers Contract requirement = %#v", definition.Consumes)
	}
	assertProviderCatalogCurrent(t, "github.com/JairusSW/pool/register", providers)
}

func assertProviderCatalogCurrent(t *testing.T, importPath string, providers []wago.PluginProvider) {
	t.Helper()
	want, err := wago.EncodeProviderCatalog(importPath, providers)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(wago.ProviderCatalogFile)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s is stale; run wago plugin catalog", wago.ProviderCatalogFile)
	}
	if _, err := wago.DecodeProviderCatalog(got); err != nil {
		t.Fatalf("%s: %v", wago.ProviderCatalogFile, err)
	}
}

func assertManifestMetadata(t *testing.T, manifest manifestPackage, definition wago.PluginDefinition) {
	t.Helper()
	authors := make([]string, len(manifest.Authors))
	for i := range manifest.Authors {
		authors[i] = manifest.Authors[i].Name
	}
	if manifest.Module != definition.ID ||
		manifest.Version != definition.Version ||
		manifest.Name != definition.Name ||
		manifest.Description != definition.Description ||
		manifest.Stability != definition.Stability ||
		manifest.License != definition.Provenance.License ||
		manifest.Homepage != definition.Provenance.Homepage ||
		manifest.Repository != definition.Provenance.Repository ||
		!reflect.DeepEqual(authors, definition.Provenance.Authors) ||
		!reflect.DeepEqual(manifest.Engines, definition.Compatibility.Engines) ||
		!reflect.DeepEqual(manifest.Platforms, definition.Compatibility.Platforms) {
		t.Fatalf("manifest metadata drifted\nmanifest=%#v\ndefinition=%#v", manifest, definition)
	}
}

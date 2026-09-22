// Package testutil holds helpers shared by tests across packages.
package testutil

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
)

// RepoRoot walks up from the working directory to the directory with go.mod.
func RepoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// ConfigPath is the Rivergate instance config.
func ConfigPath(t testing.TB) string {
	return filepath.Join(RepoRoot(t), "config", "rivergate.yaml")
}

// Config loads the Rivergate instance config.
func Config(t testing.TB) *config.Config {
	t.Helper()
	c, err := config.Load(ConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Fixture is one demo ticket file.
type Fixture struct {
	Name    string
	Payload []byte
}

// Fixtures returns the demo ticket fixtures in file-name order.
func Fixtures(t testing.TB) []Fixture {
	t.Helper()
	dir := filepath.Join(RepoRoot(t), "demo", "tickets")
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(matches)
	var out []Fixture
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, Fixture{Name: filepath.Base(m), Payload: b})
	}
	if len(out) == 0 {
		t.Fatal("no fixtures found")
	}
	return out
}

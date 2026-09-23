package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Memory is an in-memory Store for tests.
type Memory struct {
	mu    sync.RWMutex
	items map[string]Item
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{items: map[string]Item{}} }

func (m *Memory) Get(_ context.Context, id string) (Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	it, ok := m.items[id]
	if !ok || it.Expired(time.Now()) {
		return Item{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return clone(it)
}

func (m *Memory) Put(_ context.Context, it Item) error {
	c, err := clone(it)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[it.ID] = c
	return nil
}

func (m *Memory) List(_ context.Context, f Filter) ([]Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Item
	for _, it := range m.items {
		if f.match(it) {
			c, err := clone(it)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
	}
	sortItems(out)
	return out, nil
}

// clone deep-copies via JSON so callers can't mutate stored state, and so the
// in-memory store behaves like a real serialized store.
func clone(it Item) (Item, error) {
	b, err := json.Marshal(it)
	if err != nil {
		return Item{}, err
	}
	var c Item
	err = json.Unmarshal(b, &c)
	return c, err
}

// File stores one JSON document per item under dir. Writes are atomic
// (temp file + rename) so the CLI, worker and dashboard can share a data dir.
type File struct {
	dir string
}

var validID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// NewFile creates the items directory.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &File{dir: dir}, nil
}

func (f *File) path(id string) (string, error) {
	if !validID.MatchString(id) {
		return "", fmt.Errorf("%w: invalid id %q", ErrNotFound, id)
	}
	return filepath.Join(f.dir, id+".json"), nil
}

func (f *File) Get(_ context.Context, id string) (Item, error) {
	p, err := f.path(id)
	if err != nil {
		return Item{}, err
	}
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return Item{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Item{}, err
	}
	var it Item
	if err := json.Unmarshal(b, &it); err != nil {
		return Item{}, fmt.Errorf("decode %s: %w", id, err)
	}
	if it.Expired(time.Now()) {
		return Item{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return it, nil
}

func (f *File) Put(_ context.Context, it Item) error {
	p, err := f.path(it.ID)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(it, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(f.dir, ".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (f *File) List(ctx context.Context, flt Filter) ([]Item, error) {
	es, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	var out []Item
	for _, e := range es {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		it, err := f.Get(ctx, strings.TrimSuffix(name, ".json"))
		if errors.Is(err, ErrNotFound) {
			continue // expired
		}
		if err != nil {
			return nil, err
		}
		if flt.match(it) {
			out = append(out, it)
		}
	}
	sortItems(out)
	return out, nil
}

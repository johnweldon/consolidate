package factory

import (
	"sync"

	"github.com/johnweldon/consolidate/storage"
)

// Registry holds the registered factory methods for Repositories
var Registry = registrar{}

type registrar struct {
	sync.Mutex
	once     sync.Once
	registry map[string]func() storage.Repository
}

func (r *registrar) Add(name string, fn func() storage.Repository) {
	r.Lock()
	defer r.Unlock()
	r.once.Do(r.initialize)

	r.registry[name] = fn
}

func (r *registrar) Create(name string) storage.Repository {
	r.Lock()
	defer r.Unlock()
	r.once.Do(r.initialize)

	if fn, ok := r.registry[name]; ok {
		return fn()
	}
	return nil
}

func (r *registrar) initialize() {
	r.registry = map[string]func() storage.Repository{}
}

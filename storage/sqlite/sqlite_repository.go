package sqlite

import (
	"github.com/johnweldon/consolidate/storage"
	"github.com/johnweldon/consolidate/storage/factory"
)

func init() {
	factory.Registry.Add("sqlite", newRepository)
}

func newRepository() storage.Repository {
	return &repository{}
}

type repository struct{}

func (r *repository) AllNames() []string { return nil }
func (r *repository) AllTags() []string  { return nil }

func (r *repository) AddFile(file string, root string) error {
	obj, err := storage.NewObject(file, root)
	if err != nil {
		return err
	}
	return r.Add(obj)
}

func (r *repository) Add(o storage.Object) error {
	return nil
}

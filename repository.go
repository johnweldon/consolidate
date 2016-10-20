package main

import (
	"fmt"
	"strings"
	"sync"
)

// Repository is the overall storage unit
type Repository interface {
	AddFile(file string, root string) error
	AllNames() []string
	AllTags() []string
}

// NewRepository returns an initialized Repository
func NewRepository() Repository {
	return &repository{
		Objects: map[hashKey]*object{},
		Names:   map[string]map[hashKey]*object{},
		Tags:    map[string]map[hashKey]*object{},
	}
}

type repository struct {
	sync.Mutex
	originalSize   uint64
	compressedSize uint64
	Objects        map[hashKey]*object
	Names          map[string]map[hashKey]*object
	Tags           map[string]map[hashKey]*object
}

func (r *repository) Object(key hashKey) *object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return r.Objects[key]
}

func (r *repository) AllNames() []string {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return listKeys(r.Names)
}

func (r *repository) AllTags() []string {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return listKeys(r.Tags)
}

func (r *repository) ObjectsByName(name string) []*object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return listObjects(name, r.Names)
}

func (r *repository) ObjectsByTag(tag string) []*object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return listObjects(tag, r.Tags)
}

func (r *repository) AddFile(file string, root string) error {
	obj, err := hashFile(file, root)
	if err != nil {
		return err
	}
	return r.Add(obj)
}

func (r *repository) Add(o *object) error {
	if r == nil || o == nil {
		return fmt.Errorf("nil object")
	}
	r.Lock()
	defer r.Unlock()

	existing, ok := r.Objects[o.Hash]
	if !ok {
		r.Objects[o.Hash] = o
		for name := range o.Names {
			r.Names[name] = map[hashKey]*object{o.Hash: o}
		}
		for tag := range o.Tags {
			r.Tags[tag] = map[hashKey]*object{o.Hash: o}
		}
		r.originalSize += uint64(o.Size)
		r.compressedSize += uint64(len(o.compressedData))
		return nil
	}

	if existing.Size != o.Size {
		return fmt.Errorf("hash collision %v and %v", existing.Names, o.Names)
	}

	r.originalSize += uint64(o.Size)

	for name := range o.Names {
		existing.Names[name] = struct{}{}
		addKeyToRepo(name, o, r.Names)
	}

	for tag := range o.Tags {
		existing.Tags[tag] = struct{}{}
		addKeyToRepo(tag, o, r.Tags)
	}

	return nil
}

func (r *repository) Remove(key hashKey) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	var ok bool
	var obj *object
	if obj, ok = r.Objects[key]; !ok {
		return
	}
	delete(r.Objects, key)
	for name := range obj.Names {
		if m, ok := r.Names[name]; ok {
			delete(m, key)
		}
	}
	for tag := range obj.Tags {
		if m, ok := r.Tags[tag]; ok {
			delete(m, key)
		}
	}
}

func (r *repository) String() string {
	show := []string{
		r.totalObjects(),
		r.totalNames(),
		r.totalTags(),
		r.originalSizeStr(),
		r.compressedSizeStr(),
		r.compressionRatio(),
	}
	return strings.Join(show, "\n")
}

func listKeys(m map[string]map[hashKey]*object) []string {
	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func listObjects(key string, m map[string]map[hashKey]*object) []*object {
	objects := []*object{}
	for _, obj := range m[key] {
		objects = append(objects, obj)
	}
	return objects
}

func addKeyToRepo(key string, o *object, repo map[string]map[hashKey]*object) {
	if p, ok := repo[key]; ok {
		p[o.Hash] = o
	} else {
		repo[key] = map[hashKey]*object{o.Hash: o}
	}
}

func (r *repository) totalObjects() string {
	return fmt.Sprintf("%-20s: %d", "Total Objects", len(r.Objects))
}
func (r *repository) totalNames() string {
	return fmt.Sprintf("%-20s: %d", "Total Names", len(r.AllNames()))
}
func (r *repository) totalTags() string {
	return fmt.Sprintf("%-20s: %d", "Total Tags", len(r.AllTags()))
}
func (r *repository) originalSizeStr() string {
	return fmt.Sprintf("%-20s: %d", "Original Size", r.originalSize)
}
func (r *repository) compressedSizeStr() string {
	return fmt.Sprintf("%-20s: %d", "Compressed Size", r.compressedSize)
}
func (r *repository) compressionRatio() string {
	return fmt.Sprintf("%-20s: %f", "Compression Ratio", float64(r.originalSize)/float64(r.compressedSize))
}

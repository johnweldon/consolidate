package memory

import (
	"fmt"
	"strings"
	"sync"

	"github.com/johnweldon/consolidate/storage"
	"github.com/johnweldon/consolidate/storage/factory"
)

func init() {
	factory.Registry.Add("memory", newRepository)
}

func newRepository() storage.Repository {
	return &repository{
		Objects: map[uint64]storage.Object{},
		Names:   map[string]map[uint64]storage.Object{},
		Tags:    map[string]map[uint64]storage.Object{},
	}
}

type repository struct {
	sync.Mutex
	originalSize   uint64
	compressedSize uint64
	Objects        map[uint64]storage.Object
	Names          map[string]map[uint64]storage.Object
	Tags           map[string]map[uint64]storage.Object
}

func (r *repository) Object(key uint64) storage.Object {
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

func (r *repository) ObjectsByName(name string) []storage.Object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return listObjects(name, r.Names)
}

func (r *repository) ObjectsByTag(tag string) []storage.Object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return listObjects(tag, r.Tags)
}

func (r *repository) AddFile(file string, root string) error {
	obj, err := storage.NewObject(file, root)
	if err != nil {
		return err
	}
	return r.Add(obj)
}

func (r *repository) Add(o storage.Object) error {
	if r == nil || o == nil {
		return fmt.Errorf("nil storage.Object")
	}
	r.Lock()
	defer r.Unlock()

	existing, ok := r.Objects[o.Hash()]
	if !ok {
		r.Objects[o.Hash()] = o
		for _, name := range o.Names() {
			r.Names[name] = map[uint64]storage.Object{o.Hash(): o}
		}
		for _, tag := range o.Tags() {
			r.Tags[tag] = map[uint64]storage.Object{o.Hash(): o}
		}
		r.originalSize += uint64(o.Size())
		r.compressedSize += uint64(o.CompressedSize())
		return nil
	}

	if existing.Size() != o.Size() {
		return fmt.Errorf("hash collision %v and %v", existing.Names(), o.Names())
	}

	r.originalSize += uint64(o.Size())

	for _, name := range o.Names() {
		existing.AddName(name)
		addKeyToRepo(name, o, r.Names)
	}

	for _, tag := range o.Tags() {
		existing.AddTag(tag)
		addKeyToRepo(tag, o, r.Tags)
	}

	return nil
}

func (r *repository) Remove(key uint64) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	var ok bool
	var obj storage.Object
	if obj, ok = r.Objects[key]; !ok {
		return
	}
	delete(r.Objects, key)
	for _, name := range obj.Names() {
		if m, ok := r.Names[name]; ok {
			delete(m, key)
		}
	}
	for _, tag := range obj.Tags() {
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

func listKeys(m map[string]map[uint64]storage.Object) []string {
	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func listObjects(key string, m map[string]map[uint64]storage.Object) []storage.Object {
	objects := []storage.Object{}
	for _, obj := range m[key] {
		objects = append(objects, obj)
	}
	return objects
}

func addKeyToRepo(key string, o storage.Object, repo map[string]map[uint64]storage.Object) {
	if p, ok := repo[key]; ok {
		p[o.Hash()] = o
	} else {
		repo[key] = map[uint64]storage.Object{o.Hash(): o}
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

package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type Hash uint64

func HashFile(file string) (*Object, error) {
	var err error
	var fd *os.File
	var b bytes.Buffer
	var sz int64

	name := path.Clean(file)
	if fd, err = os.Open(name); err != nil {
		return nil, err
	}
	defer fd.Close()

	h := fnv.New64a()
	tw := io.MultiWriter(h, zlib.NewWriter(&b))
	if sz, err = io.Copy(tw, fd); err != nil {
		return nil, err
	}

	tags := map[string]interface{}{}
	dir := name
	var elem string
	for {
		dir, elem = path.Split(dir)
		tags[elem] = nil
		if dir == "" {
			break
		}
		dir = dir[:len(dir)-1]
	}

	return &Object{
		Hash:           Hash(h.Sum64()),
		Names:          map[string]interface{}{name: nil},
		Tags:           tags,
		Size:           sz,
		compressedData: b.Bytes(),
	}, nil
}

type Object struct {
	Hash           Hash
	Names          map[string]interface{}
	Tags           map[string]interface{}
	Size           int64
	compressedData []byte
}

func (o *Object) Data(dest io.Writer) error {
	if o == nil {
		return fmt.Errorf("nil object")
	}
	var r io.Reader
	var err error
	b := bytes.NewBuffer(o.compressedData)
	if r, err = zlib.NewReader(b); err == nil {
		_, err = io.Copy(dest, r)
	}
	return err
}

type Repository struct {
	sync.Mutex
	originalSize   uint64
	compressedSize uint64
	Objects        map[Hash]*Object
	Names          map[string]map[Hash]*Object
	Tags           map[string]map[Hash]*Object
}

func NewRepository() *Repository {
	return &Repository{
		Objects: map[Hash]*Object{},
		Names:   map[string]map[Hash]*Object{},
		Tags:    map[string]map[Hash]*Object{},
	}
}

func (r *Repository) AllNames() []string {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	names := []string{}
	for name := range r.Names {
		names = append(names, name)
	}
	return names
}

func (r *Repository) AllTags() []string {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	tags := []string{}
	for tag := range r.Tags {
		tags = append(tags, tag)
	}
	return tags
}

func (r *Repository) Object(hash Hash) *Object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	return r.Objects[hash]
}

func (r *Repository) ObjectsByName(name string) []*Object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	objects := []*Object{}
	for _, obj := range r.Names[name] {
		objects = append(objects, obj)
	}
	return objects
}

func (r *Repository) ObjectsByTag(tag string) []*Object {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()

	objects := []*Object{}
	for _, obj := range r.Names[tag] {
		objects = append(objects, obj)
	}
	return objects
}

func (r *Repository) AddFile(file string) error {
	if obj, err := HashFile(file); err == nil {
		return r.Add(obj)
	} else {
		return err
	}
}

func (r *Repository) Add(o *Object) error {
	if r == nil || o == nil {
		return fmt.Errorf("nil object")
	}
	r.Lock()
	defer r.Unlock()

	existing, ok := r.Objects[o.Hash]
	if !ok {
		r.Objects[o.Hash] = o
		for name := range o.Names {
			r.Names[name] = map[Hash]*Object{o.Hash: o}
		}
		for tag := range o.Tags {
			r.Tags[tag] = map[Hash]*Object{o.Hash: o}
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
		if _, ok := existing.Names[name]; !ok {
			existing.Names[name] = nil
		}
		if prev, ok := r.Names[name]; !ok {
			r.Names[name] = map[Hash]*Object{o.Hash: o}
		} else {
			prev[o.Hash] = o
		}
	}

	for tag := range o.Tags {
		if _, ok := existing.Tags[tag]; !ok {
			existing.Tags[tag] = nil
		}
		if prev, ok := r.Tags[tag]; !ok {
			r.Tags[tag] = map[Hash]*Object{o.Hash: o}
		} else {
			prev[o.Hash] = o
		}
	}

	return nil
}

func (r *Repository) Remove(hash Hash) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	var ok bool
	var obj *Object
	if obj, ok = r.Objects[hash]; !ok {
		return
	}
	delete(r.Objects, hash)
	for name := range obj.Names {
		if m, ok := r.Names[name]; ok {
			delete(m, hash)
		}
	}
	for tag := range obj.Tags {
		if m, ok := r.Tags[tag]; ok {
			delete(m, hash)
		}
	}
}

func (r *Repository) totalObjects() string {
	return fmt.Sprintf("%-20s: %d", "Total Objects", len(r.Objects))
}
func (r *Repository) totalNames() string {
	return fmt.Sprintf("%-20s: %d", "Total Names", len(r.AllNames()))
}
func (r *Repository) totalTags() string {
	return fmt.Sprintf("%-20s: %d", "Total Tags", len(r.AllTags()))
}
func (r *Repository) originalSizeStr() string {
	return fmt.Sprintf("%-20s: %d", "Original Size", r.originalSize)
}
func (r *Repository) compressedSizeStr() string {
	return fmt.Sprintf("%-20s: %d", "Compressed Size", r.compressedSize)
}
func (r *Repository) compressionRatio() string {
	return fmt.Sprintf("%-20s: %f", "Compression Ratio", float64(r.originalSize)/float64(r.compressedSize))
}

func (r *Repository) String() string {
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

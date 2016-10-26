package storage

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"path"
)

// NewObject builds an Object from a file, and uses the root to build
// tags out of leaf folders
func NewObject(file string, root string) (Object, error) {
	var err error
	var fd *os.File
	var b bytes.Buffer
	var sz int64

	root = path.Clean(root)
	name := path.Clean(file)
	suffix := name[len(root):]
	if fd, err = os.Open(name); err != nil {
		return nil, err
	}
	defer func(fname string) {
		if e := fd.Close(); e != nil {
			fmt.Fprintf(os.Stderr, "error closing %q: %v\n", fname, e)
		}
	}(file)

	h := hashStrategy()
	tw := io.MultiWriter(h, compressStrategy(&b))
	if sz, err = io.Copy(tw, fd); err != nil {
		return nil, err
	}

	tags := map[string]interface{}{}
	dir, elem := path.Split(suffix)
	for {
		dir, elem = path.Split(dir)
		if elem != "" {
			tags[elem] = nil
		}
		if dir == "" {
			break
		}
		dir = dir[:len(dir)-1]
	}

	return &rawObject{
		hash:   h.Sum64(),
		names:  map[string]interface{}{name: nil},
		tags:   tags,
		size:   uint64(sz),
		zzData: b.Bytes(),
	}, nil
}

// Object is the basic interface for Repository objects
type Object interface {
	Hash() uint64
	Names() []string
	Tags() []string
	Size() uint64
	CompressedSize() uint64
	AddName(name string)
	AddTag(tag string)
	WriteData(dest io.Writer, decompress bool) error
	RawData() []byte
}

var hashStrategy = fnvHasher
var compressStrategy = zlibCompress

type rawObject struct {
	hash   uint64
	names  map[string]interface{}
	tags   map[string]interface{}
	size   uint64
	zzData []byte
}

func (o *rawObject) Hash() uint64           { return o.hash }
func (o *rawObject) Names() []string        { return mapKeys(o.names) }
func (o *rawObject) Tags() []string         { return mapKeys(o.tags) }
func (o *rawObject) Size() uint64           { return o.size }
func (o *rawObject) CompressedSize() uint64 { return uint64(len(o.zzData)) }
func (o *rawObject) AddName(name string)    { o.names[name] = struct{}{} }
func (o *rawObject) AddTag(tag string)      { o.tags[tag] = struct{}{} }

func mapKeys(m map[string]interface{}) []string {
	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func (o *rawObject) WriteData(dest io.Writer, decompress bool) error {
	if o == nil {
		return fmt.Errorf("nil object")
	}
	b := bytes.NewBuffer(o.zzData)
	var r io.Reader = b
	var err error
	if decompress {
		if r, err = zlib.NewReader(b); err != nil {
			return err
		}
	}
	_, err = io.Copy(dest, r)
	return err
}

func (o *rawObject) RawData() []byte {
	if o == nil {
		return nil
	}
	return o.zzData
}

func fnvHasher() hash.Hash64             { return fnv.New64a() }
func zlibCompress(w io.Writer) io.Writer { return zlib.NewWriter(w) }

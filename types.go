package main

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

var hashStrategy = fnvHasher
var compressStrategy = zlibCompress

func hashFile(file string, root string) (*object, error) {
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

	return &object{
		Hash:           hashKey(h.Sum64()),
		Names:          map[string]interface{}{name: nil},
		Tags:           tags,
		Size:           sz,
		compressedData: b.Bytes(),
	}, nil
}

type hashKey uint64

type object struct {
	Hash           hashKey
	Names          map[string]interface{}
	Tags           map[string]interface{}
	Size           int64
	compressedData []byte
}

func (o *object) Data(dest io.Writer) error {
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

func fnvHasher() hash.Hash64             { return fnv.New64a() }
func zlibCompress(w io.Writer) io.Writer { return zlib.NewWriter(w) }

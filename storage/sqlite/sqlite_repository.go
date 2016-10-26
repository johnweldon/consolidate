package sqlite

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/johnweldon/consolidate/storage"
	"github.com/johnweldon/consolidate/storage/factory"
)

func init() {
	factory.Registry.Add("sqlite", newRepository)
}

func newRepository() storage.Repository {
	path := ".consolidate.db"
	return &repository{
		path: path,
	}
}

func (r *repository) init() {
	if r.err != nil {
		return
	}
	db, err := sql.Open("sqlite3", r.path)
	if err != nil {
		r.err = err
		return
	}
	defer db.Close()
	stmts := []string{
		`create table if not exists objects (id integer not null primary key, size integer, data blob)`,
		`create table if not exists names (id integer not null, name text)`,
		`create table if not exists tags (id integer not null, tag text)`,
	}
	for _, stmt := range stmts {
		if _, err = db.Exec(stmt); err != nil {
			r.err = err
			return
		}
	}
}

type repository struct {
	m   sync.Mutex
	o   sync.Once
	err error

	path string
}

func (r *repository) error() error {
	if r == nil {
		return fmt.Errorf("repository is nil")
	}
	r.o.Do(r.init)
	return r.err
}

func (r *repository) AllNames() []string {
	if err := r.error(); err != nil {
		return nil
	}
	db, err := sql.Open("sqlite3", r.path)
	if err != nil {
		r.err = err
		return nil
	}
	defer db.Close()

	rows, err := db.Query(`select name from names`)
	if err != nil {
		r.err = err
		return nil
	}

	names := []string{}
	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		if err != nil {
			r.err = err
			return nil
		}
		names = append(names, name)
	}

	return names
}

func (r *repository) AllTags() []string {
	if err := r.error(); err != nil {
		return nil
	}
	db, err := sql.Open("sqlite3", r.path)
	if err != nil {
		r.err = err
		return nil
	}
	defer db.Close()

	rows, err := db.Query(`select tag from tags`)
	if err != nil {
		r.err = err
		return nil
	}

	tags := []string{}
	var tag string
	for rows.Next() {
		err = rows.Scan(&tag)
		if err != nil {
			r.err = err
			return nil
		}
		tags = append(tags, tag)
	}

	return tags
}

func (r *repository) AddFile(file string, root string) error {
	if err := r.error(); err != nil {
		return err
	}

	obj, err := storage.NewObject(file, root)
	if err != nil {
		return err
	}
	return r.Add(obj)
}

func (r *repository) Add(o storage.Object) error {
	if err := r.error(); err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", r.path)
	if err != nil {
		r.err = err
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare(`insert or ignore into objects (id, size, data) values (?, ?, ?)`)
	if err != nil {
		r.err = err
		return err
	}

	_, err = stmt.Exec(int64(o.Hash()), o.Size(), o.RawData())
	if err != nil {
		r.err = err
		return err
	}

	stmt, err = db.Prepare(`insert or ignore into names (id, name) values (?, ?)`)
	if err != nil {
		r.err = err
		return err
	}

	for _, name := range o.Names() {
		_, err = stmt.Exec(int64(o.Hash()), name)
		if err != nil {
			r.err = err
			return err
		}
	}

	stmt, err = db.Prepare(`insert or ignore into tags (id, tag) values (?, ?)`)
	if err != nil {
		r.err = err
		return err
	}

	for _, tag := range o.Tags() {
		_, err = stmt.Exec(int64(o.Hash()), tag)
		if err != nil {
			r.err = err
			return err
		}
	}
	return nil
}

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

type repository struct {
	sync.Mutex
	o   sync.Once
	err error

	path string
	db   *sql.DB
}

func (r *repository) AllNames() []string {
	if err := r.error(); err != nil {
		return nil
	}

	rows, err := r.db.Query(`select distinct name from names order by name`)
	if err != nil {
		r.err = err
		return nil
	}
	defer rows.Close()

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

	rows, err := r.db.Query(`select distinct tag from tags order by tag`)
	if err != nil {
		r.err = err
		return nil
	}
	defer rows.Close()

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

	stmt, err := r.db.Prepare(`insert or ignore into objects (id, size, data) values (?, ?, ?)`)
	if err != nil {
		r.err = err
		return err
	}

	r.Lock()
	defer r.Unlock()

	_, err = stmt.Exec(int64(o.Hash()), o.Size(), o.RawData())
	if err != nil {
		r.err = err
		return err
	}

	stmt, err = r.db.Prepare(`insert or ignore into names (id, name) values (?, ?)`)
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
	if err = stmt.Close(); err != nil {
		r.err = err
		return err
	}

	stmt, err = r.db.Prepare(`insert or ignore into tags (id, tag) values (?, ?)`)
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
	if err = stmt.Close(); err != nil {
		r.err = err
		return err
	}
	return nil
}

func (r *repository) init() {
	if r.err != nil {
		return
	}
	db, err := sql.Open("sqlite3", r.path+"?_busy_timeout=5000&mode=rwc&cache=shared")
	if err != nil {
		r.err = err
		return
	}
	r.db = db

	stmts := []string{
		`create table if not exists objects (id integer not null primary key, size integer, data blob)`,
		`create table if not exists names (id integer not null, name text)`,
		`create table if not exists tags (id integer not null, tag text)`,
	}

	for _, stmt := range stmts {
		if _, err = r.db.Exec(stmt); err != nil {
			r.err = err
			return
		}
	}
}

func (r *repository) error() error {
	if r == nil {
		return fmt.Errorf("repository is nil")
	}
	r.o.Do(r.init)
	if r.db == nil {
		r.err = fmt.Errorf("db not initialized")
	}
	return r.err
}

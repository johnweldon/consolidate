package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/urfave/cli"

	"github.com/johnweldon/consolidate/storage"
	"github.com/johnweldon/consolidate/storage/factory"
	_ "github.com/johnweldon/consolidate/storage/memory"
	_ "github.com/johnweldon/consolidate/storage/sqlite"
)

func appMain(c *cli.Context) error {
	from := c.StringSlice("source")
	if len(from) < 1 {
		if err := cli.ShowAppHelp(c); err != nil {
			return err
		}
		return fmt.Errorf("no source folders specified")
	}

	out, e, quit := make(chan string), make(chan error), make(chan interface{})
	lctx := logContext{C: c, O: out, E: e, Q: quit}
	ctx := appContext{C: c, R: factory.Registry.Create("sqlite"), O: out, E: e}

	go lctx.logger()

	var wg sync.WaitGroup
	for _, dir := range from {
		if stat, err := os.Stat(dir); err != nil {
			e <- fmt.Errorf("error %q: %v", dir, err)
			continue
		} else if !stat.IsDir() {
			e <- fmt.Errorf("%q is not a folder", dir)
			continue
		}
		wg.Add(1)
		go func(begin string) {
			defer wg.Done()
			if err := filepath.Walk(begin, ctx.visitor(begin)); err != nil {
				e <- err
			}
		}(dir)
	}
	wg.Wait()

	quit <- nil
	close(e)
	close(out)
	close(quit)

	if c.Bool("verbose") {
		fmt.Printf("\nNAMES: %v\n\n", ctx.R.AllNames())
		fmt.Printf(" TAGS: %v\n\n", ctx.R.AllTags())
	}
	return nil
}

type logContext struct {
	C *cli.Context
	O <-chan string
	E <-chan error
	Q <-chan interface{}
}

func (c logContext) logger() {
	verbose := c.C.Bool("verbose")

	for {
		select {
		case msg := <-c.O:
			if verbose && msg != "" {
				fmt.Printf("LOG: %s\n", msg)
			}
		case err := <-c.E:
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERR: %v\n", err)
			}
		case <-c.Q:
			if verbose {
				fmt.Println("QUIT")
			}
			return
		}
	}
}

type appContext struct {
	C *cli.Context
	R storage.Repository
	O chan<- string
	E chan<- error
}

func (c appContext) visitor(root string) func(string, os.FileInfo, error) error {
	exclude := c.C.StringSlice("exclude")

	return func(path string, f os.FileInfo, e error) error {
		if f.IsDir() {
			return nil
		}
		for _, ex := range exclude {
			if strings.Contains(path, ex) {
				return nil
			}
		}
		if err := c.R.AddFile(path, root); err != nil {
			c.E <- err
			return nil
		}
		c.O <- "added: " + path
		return nil
	}
}

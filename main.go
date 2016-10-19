package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/urfave/cli"
)

func Main(c *cli.Context) error {
	from := c.StringSlice("source")
	if len(from) < 1 {
		return fmt.Errorf("no source folders specified")
	}

	exclude := c.StringSlice("exclude")
	verbose := c.Bool("verbose")

	r := NewRepository()
	defer fmt.Printf("Repository:\n%s\n", r)

	outLog, errLog, quit := make(chan string), make(chan error), make(chan interface{})

	go func() {
		for {
			select {
			case msg := <-outLog:
				if verbose && msg != "" {
					fmt.Printf("LOG: %s\n", msg)
				}
			case err := <-errLog:
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERR: %v\n", err)
				}
			case <-quit:
				if verbose {
					fmt.Printf("QUIT\n")
				}
				break
			}
		}
	}()

	visitor := func(root string) func(string, os.FileInfo, error) error {
		return func(path string, f os.FileInfo, e error) error {
			if f.IsDir() {
				return nil
			}
			for _, ex := range exclude {
				if strings.Contains(path, ex) {
					return nil
				}
			}
			if err := r.AddFile(path, root); err != nil {
				errLog <- err
				return nil
			}
			outLog <- "added: " + path
			return nil
		}
	}

	var wg sync.WaitGroup
	for _, dir := range from {
		if stat, err := os.Stat(dir); err != nil {
			errLog <- fmt.Errorf("error %q: %v", dir, err)
			continue
		} else if !stat.IsDir() {
			errLog <- fmt.Errorf("%q is not a folder", dir)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := filepath.Walk(dir, visitor(dir)); err != nil {
				errLog <- err
			}
		}()
	}
	wg.Wait()
	close(errLog)
	close(outLog)
	quit <- nil

	if verbose {
		fmt.Printf("\nNAMES: %v\n\n", r.AllNames())
		fmt.Printf(" TAGS: %v\n\n", r.AllTags())
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Action = Main
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:  "source, s",
			Usage: "source folder(s) to backup",
		},
		cli.StringSliceFlag{
			Name:  "exclude, x",
			Usage: "folder(s) to exclude", //TODO:better
		},
		cli.BoolFlag{
			Name:  "verbose, V",
			Usage: "verbose output",
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

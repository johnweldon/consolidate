package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli"
)

func Main(c *cli.Context) error {
	from := c.StringSlice("source")
	if len(from) < 1 {
		return fmt.Errorf("no source folders specified")
	}

	exclude := c.StringSlice("exclude")

	r := NewRepository()
	defer fmt.Printf("Repository: %s\n", r)

	for _, dir := range from {
		if stat, err := os.Stat(dir); err != nil {
			return err
		} else if !stat.IsDir() {
			return fmt.Errorf("%q is not a folder", dir)
		}
		visitor := func(path string, f os.FileInfo, e error) error {
			if f.IsDir() {
				return e
			}
			for _, ex := range exclude {
				if strings.Contains(path, ex) {
					return e
				}
			}
			if err := r.AddFile(path); err != nil {
				return err
			}
			return e
		}

		if err := filepath.Walk(dir, visitor); err != nil {
			return err
		}
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
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

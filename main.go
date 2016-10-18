package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func Main(c *cli.Context) error {
	from := c.StringSlice("source")
	if len(from) < 1 {
		return fmt.Errorf("no source folders specified")
	}
	for _, dir := range from {
		if stat, err := os.Stat(dir); err != nil {
			return err
		} else if !stat.IsDir() {
			return fmt.Errorf("%q is not a folder", dir)
		}
		fmt.Printf("archiving %q ...\n", dir)
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
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

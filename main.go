package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Action = appMain
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

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/castaneai/spankeys"

	"cloud.google.com/go/spanner"
	"github.com/castaneai/spancompare"
	spcli "github.com/castaneai/spancompare/cli"

	"github.com/urfave/cli"
)

const (
	Name    = "spancompare"
	Version = "0.0.1"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix(fmt.Sprintf("[%s] ", Name))

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "Display version information and exit",
	}

	app := cli.NewApp()
	app.Name = Name
	app.Version = Version
	app.Usage = "Compare two Cloud Spanner tables"
	app.UsageText = fmt.Sprintf("%s [global options] table1:table2", app.Name)
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "server1",
			Usage:    "Connection information for the first server of Cloud Spanner (format: projects/xxx/instances/yyy/databases/zzz)",
			Required: true,
		},
		cli.StringFlag{
			Name:     "server2",
			Usage:    "Connection information for the second server of Cloud Spanner (format: projects/xxx/instances/yyy/databases/zzz)",
			Required: true,
		},
		cli.StringFlag{
			Name:  "changes-for",
			Usage: "Controls the direction of the difference",
			Value: "server1",
		},
		cli.StringFlag{
			Name:  "difftype",
			Usage: `How to display diff-style output, "unified" or "sql"`,
			Value: "unified",
		},
	}
	app.Action = cmdMain
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func cmdMain(c *cli.Context) error {
	ctx := context.Background()

	c1, err := spanner.NewClient(ctx, c.GlobalString("server1"))
	if err != nil {
		return err
	}
	defer c1.Close()

	c2, err := spanner.NewClient(ctx, c.GlobalString("server2"))
	if err != nil {
		return err
	}
	defer c2.Close()

	rows1, err := ds1.Rows(ctx)
	if err != nil {
		return err
	}
	rows2, err := ds2.Rows(ctx)
	if err != nil {
		return err
	}
	cmp := &spancompare.DefaultRowComparator{IgnoreColumns: c.GlobalStringSlice("ignore")}
	rd, err := spancompare.CompareRows(rows1, rows2, cmp)
	if err != nil {
		return err
	}
	if !rd.HasDiff() {
		log.Printf("No diff found")
		return nil
	}

	// TODO: schema diff between server1 and server2
	cols, err := spankeys.GetColumns(ctx, c1, table1)
	if err != nil {
		return err
	}
	var cns []string
	for _, col := range cols {
		cns = append(cns, col.Name)
	}

	ud, err := spcli.NewUnifiedDiff(c.App.Writer, cns)
	if err != nil {
		return err
	}
	diff := &spancompare.TablesDiff{
		Table1:   table1,
		Table2:   table2,
		RowsDiff: rd,
	}

	cfs := c.GlobalString("changes-for")
	if cfs != "server1" && cfs != "server2" {
		return fmt.Errorf("changesFor must be 'server1' or 'server2'")
	}
	changesFor := table1
	if cfs == "server2" {
		changesFor = table2
	}
	if err := ud.Write(diff, changesFor); err != nil {
		return err
	}
	return nil
}

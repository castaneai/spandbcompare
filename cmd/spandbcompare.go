package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/castaneai/spankeys"

	"cloud.google.com/go/spanner"
	"github.com/castaneai/spandbcompare"
	spcli "github.com/castaneai/spandbcompare/cli"

	"github.com/urfave/cli"
)

const (
	Name    = "spandbcompare"
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
	app.UsageText = fmt.Sprintf("%s [options]", app.Name)
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
	dsn1, err := spankeys.NewDSN(c.GlobalString("server1"))
	if err != nil {
		return err
	}
	dsn2, err := spankeys.NewDSN(c.GlobalString("server2"))
	if err != nil {
		return err
	}

	c1, err := spanner.NewClient(ctx, string(dsn1))
	if err != nil {
		return err
	}
	defer c1.Close()
	c2, err := spanner.NewClient(ctx, string(dsn2))
	if err != nil {
		return err
	}
	defer c2.Close()

	tables1, err := spankeys.GetTables(ctx, c1)
	if err != nil {
		return err
	}
	tables2, err := spankeys.GetTables(ctx, c1)
	if err != nil {
		return err
	}
	if len(tables1) != len(tables2) {
		return fmt.Errorf("the list of tables differs among %s and %s", dsn1, dsn2)
	}

	for _, table := range tables1 {
		ds1, err := spandbcompare.NewDataSource(ctx, c1, table.Name)
		if err != nil {
			return err
		}
		ds2, err := spandbcompare.NewDataSource(ctx, c2, table.Name)
		if err != nil {
			return err
		}

		rows1, err := ds1.Rows(ctx)
		if err != nil {
			return err
		}
		rows2, err := ds2.Rows(ctx)
		if err != nil {
			return err
		}
		cmp := &spandbcompare.DefaultRowComparator{}
		rd, err := spandbcompare.CompareRows(rows1, rows2, cmp)
		if err != nil {
			return err
		}

		cols, err := spankeys.GetColumns(ctx, c1, table.Name)
		if err != nil {
			return err
		}
		var cns []string
		for _, col := range cols {
			cns = append(cns, col.Name)
		}

		switch c.GlobalString("difftype") {
		case "sql":
			table1 := table.Name
			table2 := table.Name
			if err := showSQLDiff(c, cns, rd, table1, table2); err != nil {
				return err
			}
			break
		default:
			label1 := fmt.Sprintf("%s on %s", table.Name, dsn1)
			label2 := fmt.Sprintf("%s on %s", table.Name, dsn2)
			if err := showUnifiedDiff(c, cns, rd, label1, label2); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func showUnifiedDiff(c *cli.Context, cols []string, rd *spandbcompare.RowsDiff, label1, label2 string) error {
	cfs := c.GlobalString("changes-for")
	if cfs != "server1" && cfs != "server2" {
		return fmt.Errorf("changesFor must be 'server1' or 'server2'")
	}
	changesFor := label1
	if cfs == "server2" {
		changesFor = label2
	}

	ud, err := spcli.NewUnifiedDiff(c.App.Writer, cols, label1, label2)
	if err != nil {
		return err
	}
	if err := ud.Write(rd, changesFor); err != nil {
		return err
	}
	return nil
}

func showSQLDiff(c *cli.Context, cols []string, rd *spandbcompare.RowsDiff, table1, table2 string) error {
	cfs := c.GlobalString("changes-for")
	if cfs != "server1" && cfs != "server2" {
		return fmt.Errorf("changesFor must be 'server1' or 'server2'")
	}
	changesFor := table1
	if cfs == "server2" {
		changesFor = table2
	}

	sd, err := spandbcompare.NewSQLDiff(rd, table1, table2)
	if err != nil {
		return err
	}
	sqls, err := sd.SQL(changesFor)
	if err != nil {
		return err
	}
	for _, sql := range sqls {
		fmt.Fprintln(c.App.Writer, sql)
	}
	return nil
}

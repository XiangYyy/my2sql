package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	my "my2sql/base"
)

func main() {
	handler, _ := log.NewStreamHandler(os.Stderr)
	log.SetDefaultLogger(log.NewDefault(handler))
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()
	defer my.GConfCmd.CloseResources()
	if err := my.GConfCmd.CreateDB(); err != nil {
		return err
	}
	proceed, err := my.DiscoverBinlogs(context.Background(), my.GConfCmd,
		my.RemoteBinlogSource{Config: my.GConfCmd}, os.Stdout, os.Stderr)
	if err != nil || !proceed {
		return err
	}
	if err := my.GConfCmd.InitOutput(); err != nil {
		return err
	}
	return runPipeline(my.GConfCmd, func() error {
		if my.GConfCmd.Mode == "repl" {
			return my.ParserAllBinEventsFromRepl(my.GConfCmd)
		}
		myParser := my.BinFileParser{Parser: replication.NewBinlogParser()}
		// Keep datetime and decimal values in the formats supported by sqlbuilder.
		myParser.Parser.SetParseTime(false)
		myParser.Parser.SetUseDecimal(false)
		return myParser.MyParseAllBinlogFiles(my.GConfCmd)
	})
}

// parse owns and closes EventChan/StatChan; SQL closes only after generators exit.
func runPipeline(cfg *my.ConfCmd, parse func() error) error {
	if cfg.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(cfg, &wg)
	if cfg.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(cfg, &wg)
		for i := uint(1); i <= cfg.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, cfg, &wgGenSql)
		}
	}
	cfg.RecordError(parse())
	wgGenSql.Wait()
	close(cfg.SqlChan)
	wg.Wait()
	cfg.CloseFH()
	if err := cfg.Err(); err != nil {
		return err
	}
	log.Info("finish parsing and writing all output files")
	return nil
}

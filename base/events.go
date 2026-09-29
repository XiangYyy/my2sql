package base

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/go-log/log"
	"my2sql/constvar"
	SQL "my2sql/sqlbuilder"
	"my2sql/sqltypes"
)

type ExtraSqlInfoOfPrint struct {
	schema, table, binlog string
	startpos, endpos      uint32
	datetime              string
	trxIndex              uint64
	trxStatus             int
	gtid                  string
}

type ForwardRollbackSqlOfPrint struct {
	sqls    []string
	sqlInfo ExtraSqlInfoOfPrint
}

var (
	ForwardSqlFileNamePrefix  = "forward"
	RollbackSqlFileNamePrefix = "rollback"
)

// Keep the first failure while consumers drain producer-owned channels.
func (cfg *ConfCmd) RecordError(err error) {
	if err == nil {
		return
	}
	cfg.resultMu.Lock()
	defer cfg.resultMu.Unlock()
	if cfg.firstError == nil {
		cfg.firstError = err
	}
}

func (cfg *ConfCmd) Err() error {
	cfg.resultMu.Lock()
	defer cfg.resultMu.Unlock()
	return cfg.firstError
}

func writeOutput(w io.Writer, text string) error {
	n, err := io.WriteString(w, text)
	if err == nil && n != len(text) {
		err = io.ErrShortWrite
	}
	return err
}

func (cfg *ConfCmd) writeStats(w io.Writer, text string) {
	if cfg.Err() == nil {
		cfg.RecordError(writeOutput(w, text))
	}
}

func (cfg *ConfCmd) newOutput(name string) (io.WriteCloser, error) {
	if cfg.openOutput != nil {
		return cfg.openOutput(name)
	}
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func GenForwardRollbackSqlFromBinEvent(i uint, cfg *ConfCmd, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Infof("start thread %d to generate redo/rollback sql", i)
	for ev := range cfg.EventChan {
		if cfg.Err() != nil {
			continue
		}
		sc, err := generateSQL(cfg, ev)
		if err != nil {
			cfg.RecordError(err)
			continue
		}
		for cfg.Err() == nil {
			G_HandlingBinEventIndex.lock.Lock()
			if G_HandlingBinEventIndex.EventIdx == ev.EventIdx {
				if cfg.OutputToScreen {
					for _, sql := range sc.sqls {
						cfg.RecordError(writeOutput(os.Stdout, sql+"\n"))
					}
				} else {
					cfg.SqlChan <- sc
				}
				G_HandlingBinEventIndex.EventIdx++
				G_HandlingBinEventIndex.lock.Unlock()
				break
			}
			G_HandlingBinEventIndex.lock.Unlock()
			time.Sleep(time.Microsecond)
		}
	}
	log.Infof("exit thread %d to generate redo/rollback sql", i)
}

func generateSQL(cfg *ConfCmd, ev MyBinEvent) (sc ForwardRollbackSqlOfPrint, err error) {
	// Convert builder panics into pipeline failures so EventIdx gaps cannot strand workers.
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("generate SQL at %s: %v", ev.MyPos, v)
		}
	}()
	if !ev.IfRowsEvent || ev.BinEvent == nil || len(ev.BinEvent.Rows) == 0 {
		return sc, fmt.Errorf("missing rows at %s", ev.MyPos)
	}
	db, tb := string(ev.BinEvent.Table.Schema), string(ev.BinEvent.Table.Table)
	fulltb := GetAbsTableName(db, tb)
	tbInfo, err := G_TablesColumnsInfo.GetTableInfoJson(db, tb)
	if err != nil {
		return sc, err
	}
	if tbInfo == nil {
		return sc, fmt.Errorf("no table structure for %s", fulltb)
	}
	colCnt := len(ev.BinEvent.Rows[0])
	// This tool requires a matching historical schema; do not invent dropped columns.
	if colCnt != len(tbInfo.Columns) {
		return sc, fmt.Errorf("%s binlog columns=%d, schema columns=%d; matching historical schema required", fulltb, colCnt, len(tbInfo.Columns))
	}
	for _, row := range ev.BinEvent.Rows {
		if len(row) != colCnt {
			return sc, fmt.Errorf("inconsistent row image for %s", fulltb)
		}
	}
	if ev.SqlType == "update" && len(ev.BinEvent.Rows)%2 != 0 {
		return sc, fmt.Errorf("incomplete update row pair for %s", fulltb)
	}
	allColNames := tbInfo.Columns
	var colsDef []SQL.NonAliasColumn
	colsDef, colsTypeName := GetSqlFieldsEXpressions(colCnt, allColNames, ev.BinEvent.Table)
	colsTypeNameFromMysql := make([]string, len(colsTypeName))
	for ci, colType := range colsTypeName {
		colsTypeNameFromMysql[ci] = tbInfo.Columns[ci].FieldType
		if strings.Contains(strings.ToLower(colType), "int") && tbInfo.Columns[ci].IsUnsigned {
			for ri := range ev.BinEvent.Rows {
				ev.BinEvent.Rows[ri][ci] = sqltypes.ConvertIntUnsigned(ev.BinEvent.Rows[ri][ci], colType)
			}
		}
		if colType == "blob" && strings.Contains(strings.ToLower(tbInfo.Columns[ci].FieldType), "text") {
			for ri := range ev.BinEvent.Rows {
				if ev.BinEvent.Rows[ri][ci] == nil {
					continue
				}
				txt, ok := ev.BinEvent.Rows[ri][ci].([]byte)
				if !ok {
					return sc, fmt.Errorf("%s.%s invalid text row image", fulltb, allColNames[ci].FieldName)
				}
				ev.BinEvent.Rows[ri][ci] = string(txt)
			}
		}
	}
	uniqueKeyIdx := GetColIndexFromKey(tbInfo.GetOneUniqueKey(cfg.UseUniqueKeyFirst), allColNames)
	primaryKeyIdx := GetColIndexFromKey(tbInfo.PrimaryKey, allColNames)
	ignorePrimary := cfg.IgnorePrimaryKeyForInsert && len(primaryKeyIdx) > 0
	rollback := cfg.WorkType == "rollback"
	posStr := GetPosStr(ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
	var sqlArr []string
	switch ev.SqlType {
	case "insert":
		if rollback {
			sqlArr = GenDeleteSqlsForOneRowsEventRollbackInsert(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, cfg.SqlTblPrefixDb)
		} else {
			sqlArr = GenInsertSqlsForOneRowsEvent(posStr, ev.BinEvent, colsDef, 1, false, cfg.SqlTblPrefixDb, ignorePrimary, primaryKeyIdx)
		}
	case "delete":
		if rollback {
			sqlArr = GenInsertSqlsForOneRowsEventRollbackDelete(posStr, ev.BinEvent, colsDef, 1, cfg.SqlTblPrefixDb)
		} else {
			sqlArr = GenDeleteSqlsForOneRowsEvent(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, false, cfg.SqlTblPrefixDb)
		}
	case "update":
		sqlArr = GenUpdateSqlsForOneRowsEvent(posStr, colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, rollback, cfg.SqlTblPrefixDb)
	default:
		return sc, fmt.Errorf("unsupported SQL type %q at %s", ev.SqlType, ev.MyPos)
	}
	return ForwardRollbackSqlOfPrint{sqls: sqlArr, sqlInfo: ExtraSqlInfoOfPrint{
		schema: db, table: tb, binlog: ev.MyPos.Name, startpos: ev.StartPos, endpos: ev.MyPos.Pos,
		datetime: GetDatetimeStr(int64(ev.Timestamp), 0, constvar.DATETIME_FORMAT_NOSPACE), trxIndex: ev.TrxIndex, trxStatus: ev.TrxStatus,
		gtid: ev.Gtid,
	}}, nil
}

func PrintExtraInfoForForwardRollbackupSql(cfg *ConfCmd, wg *sync.WaitGroup) {
	defer wg.Done()
	files := map[string]io.WriteCloser{}
	buffers := map[string]*bufio.Writer{}
	rollbackFiles := []map[string]string{}
	bytesCntFiles := map[string][][]int{}
	for sc := range cfg.SqlChan {
		if cfg.Err() != nil {
			continue
		}
		rollback := cfg.WorkType == "rollback"
		name := GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, rollback, sc.sqlInfo.binlog, rollback)
		if _, ok := files[name]; !ok {
			fh, err := cfg.newOutput(name)
			if err != nil {
				cfg.RecordError(fmt.Errorf("open SQL %s: %w", name, err))
				continue
			}
			files[name], buffers[name] = fh, bufio.NewWriter(fh)
			if rollback {
				dest := GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, false)
				rollbackFiles = append(rollbackFiles, map[string]string{"tmp": name, "rollback": dest})
			}
		}
		text := GetForwardRollbackContentLineWithExtra(sc, cfg.PrintExtraInfo)
		if err := writeOutput(buffers[name], text); err != nil {
			cfg.RecordError(fmt.Errorf("write SQL %s: %w", name, err))
			continue
		}
		if rollback {
			bytesCntFiles[name] = append(bytesCntFiles[name], []int{len(text), int(sc.sqlInfo.trxIndex)})
		}
	}
	for name, fh := range files {
		if err := buffers[name].Flush(); err != nil {
			cfg.RecordError(fmt.Errorf("flush SQL %s: %w", name, err))
		}
		if err := fh.Close(); err != nil {
			cfg.RecordError(fmt.Errorf("close SQL %s: %w", name, err))
		}
	}
	if cfg.WorkType != "rollback" || cfg.Err() != nil {
		return
	}
	var reWg sync.WaitGroup
	filesChan := make(chan map[string]string, len(rollbackFiles))
	threads := GetMinValue(int(cfg.Threads), len(rollbackFiles))
	if threads == 0 && len(rollbackFiles) > 0 {
		threads = 1
	}
	for i := 1; i <= threads; i++ {
		reWg.Add(1)
		go ReverseFileGo(i, filesChan, bytesCntFiles, cfg.KeepTrx, &reWg, cfg)
	}
	for _, pair := range rollbackFiles {
		filesChan <- pair
	}
	close(filesChan)
	reWg.Wait()
	// Success is announced only by main, after stats and every worker have closed.
}

func GetForwardRollbackSqlFileName(schema, table string, filePerTable bool, outDir string, ifRollback bool, binlog string, ifTmp bool) string {
	_, idx := GetBinlogBasenameAndIndex(binlog)
	prefix := ForwardSqlFileNamePrefix
	if ifRollback {
		prefix = RollbackSqlFileNamePrefix
	}
	name := fmt.Sprintf("%s.%d.sql", prefix, idx)
	if filePerTable {
		name = fmt.Sprintf("%s.%s.%s", schema, table, name)
	}
	if ifRollback && ifTmp {
		name = "." + name
	}
	return filepath.Join(outDir, name)
}

func GetForwardRollbackContentLineWithExtra(sq ForwardRollbackSqlOfPrint, ifExtra bool) string {
	if ifExtra {
		return fmt.Sprintf("# datetime=%s database=%s table=%s binlog=%s startpos=%d stoppos=%d trxindex=%d gtid=%s\n%s;\n",
			sq.sqlInfo.datetime, sq.sqlInfo.schema, sq.sqlInfo.table, sq.sqlInfo.binlog, sq.sqlInfo.startpos, sq.sqlInfo.endpos,
			sq.sqlInfo.trxIndex, sq.sqlInfo.gtid, strings.Join(sq.sqls, ";\n"))
	}
	return strings.Join(sq.sqls, ";\n") + ";\n"
}

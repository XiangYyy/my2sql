package base

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	fileBinEventHandlingIndex uint64
	fileTrxIndex              uint64
)

type BinFileParser struct{ Parser *replication.BinlogParser }

// Time-only ranges snapshot contiguous files; other local runs default to one file.
func prepareFileBoundary(cfg *ConfCmd, first string) error {
	cfg.StartFilePos.Name = filepath.Base(cfg.StartFilePos.Name)
	if cfg.IfSetStopFilePos {
		cfg.StopFilePos.Name = filepath.Base(cfg.StopFilePos.Name)
		if cfg.StopFilePos.Pos < 4 {
			return fmt.Errorf("invalid stop position")
		}
		return nil
	}
	last := first
	if cfg.IfSetStartDateTime || cfg.IfSetStopDateTime {
		base, idx := GetBinlogBasenameAndIndex(first)
		for {
			next := filepath.Join(filepath.Dir(first), GetNextBinlog(base, idx))
			info, err := os.Stat(next)
			if os.IsNotExist(err) {
				break
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("not a binlog file: %s", next)
			}
			last = next
			idx++
		}
	}
	info, err := os.Stat(last)
	if err != nil {
		return err
	}
	if info.Size() < 4 || uint64(info.Size()) > uint64(^uint32(0)) {
		return fmt.Errorf("invalid binlog size: %d", info.Size())
	}
	cfg.StopFile, cfg.StopPos = filepath.Base(last), uint(info.Size())
	cfg.StopFilePos = mysql.Position{Name: cfg.StopFile, Pos: uint32(cfg.StopPos)}
	cfg.IfSetStopFilePos = true
	return nil
}

func (p BinFileParser) MyParseAllBinlogFiles(cfg *ConfCmd) (result error) {
	defer cfg.CloseChan()
	defer func() { cfg.RecordError(result) }()
	fileBinEventHandlingIndex, fileTrxIndex = 0, 0
	binlog, _ := GetFirstBinlogPosToParse(cfg)
	if err := prepareFileBoundary(cfg, binlog); err != nil {
		return err
	}
	base, idx := GetBinlogBasenameAndIndex(binlog)
	opened := false
	for {
		fileStart := mysql.Position{Name: filepath.Base(binlog), Pos: 4}
		cmp := fileStart.Compare(cfg.StopFilePos)
		// 排他终点只跳过后继文件；起始文件仍需打开并校验。
		if cmp == 0 && opened {
			return nil
		}
		if cmp > 0 {
			return fmt.Errorf("local file passed stop boundary %s", cfg.StopFilePos)
		}
		result, err := p.MyParseOneBinlogFile(cfg, binlog)
		if err != nil {
			return err
		}
		opened = true
		if result == C_reBreak {
			return nil
		}
		if filepath.Base(binlog) == cfg.StopFilePos.Name {
			return fmt.Errorf("binlog ended before boundary %s: %w", cfg.StopFilePos, io.ErrUnexpectedEOF)
		}
		binlog = filepath.Join(filepath.Dir(binlog), GetNextBinlog(base, idx))
		idx++
	}
}

func (p BinFileParser) MyParseOneBinlogFile(cfg *ConfCmd, name string) (code int, result error) {
	f, err := os.Open(name)
	if err != nil {
		return C_reBreak, err
	}
	defer func() {
		if err := f.Close(); result == nil && err != nil {
			result = err
		}
	}()
	header := make([]byte, 4)
	if _, err := io.ReadFull(f, header); err != nil {
		return C_reBreak, err
	}
	if !bytes.Equal(header, replication.BinLogFileHeader) {
		return C_reBreak, fmt.Errorf("%s is not a binlog file", name)
	}
	binlog := filepath.Base(name)
	return p.MyParseReader(cfg, f, &binlog)
}

func (p BinFileParser) MyParseReader(cfg *ConfCmd, r io.Reader, binlog *string) (int, error) {
	var tbMapPos uint32
	var offset uint64 = 4 // Readers start immediately after the binlog magic.
	trxStatus := 0
	currentGTID := ""
	stopAfter := false
	for !stopAfter {
		if err := cfg.Err(); err != nil {
			return C_reBreak, err
		}
		head := make([]byte, replication.EventHeaderSize)
		if _, err := io.ReadFull(r, head); err != nil {
			if err == io.EOF && *binlog != cfg.StopFilePos.Name {
				return C_reFileEnd, nil
			}
			return C_reBreak, fmt.Errorf("read %s before boundary %s: %w", *binlog, cfg.StopFilePos, err)
		}
		h, err := p.Parser.ParseHeader(head)
		if err != nil {
			return C_reBreak, err
		}
		if h.EventSize < uint32(replication.EventHeaderSize) {
			return C_reBreak, fmt.Errorf("invalid event size %d", h.EventSize)
		}
		offset += uint64(h.EventSize)
		if offset > uint64(^uint32(0)) {
			return C_reBreak, fmt.Errorf("binlog offset overflow")
		}
		// Local byte offsets prove physical progress even for ROTATE with LogPos zero.
		stopAfter, err = positionAtStop(cfg, mysql.Position{Name: *binlog, Pos: uint32(offset)})
		if err != nil {
			return C_reBreak, err
		}
		data := make([]byte, int(h.EventSize)-replication.EventHeaderSize)
		if _, err := io.ReadFull(r, data); err != nil {
			return C_reBreak, err
		}
		e, err := p.Parser.ParseEvent(h, data, append(head, data...))
		if err != nil {
			return C_reBreak, err
		}
		if h.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = h.LogPos - h.EventSize
		}
		event := &replication.BinlogEvent{Header: h, Event: e}
		// 事务组开始事件总是更新 GTID 上下文，即使事件本身被后续过滤跳过。
		if startsGtidTrxGroup(h.EventType) {
			currentGTID = formatBinlogGtid(event)
		}
		one := &MyBinEvent{MyPos: mysql.Position{Name: *binlog, Pos: h.LogPos}, StartPos: tbMapPos}
		// ROTATE must not relabel bytes that still belong to the currently opened file.
		current := *binlog
		check := one.CheckBinEvent(cfg, event, &current)
		if check == C_reBreak {
			return C_reBreak, fmt.Errorf("stopped before physical boundary %s", cfg.StopFilePos)
		}
		if check != C_reProcess {
			continue
		}
		db, tb, sqlType, sql, rowCnt := GetDbTbAndQueryAndRowCntFromBinevent(event)
		if sqlType == "query" {
			switch strings.ToLower(sql) {
			case "begin":
				trxStatus = C_trxBegin
				fileTrxIndex++
			case "commit":
				trxStatus = C_trxCommit
			case "rollback":
				trxStatus = C_trxRollback
			default:
				if one.QuerySql != nil {
					trxStatus = C_trxProcess
					rowCnt = 1
				}
			}
		} else {
			trxStatus = C_trxProcess
		}
		if cfg.WorkType != "stats" && one.IfRowsEvent {
			if _, err := G_TablesColumnsInfo.GetTableInfoJson(db, tb); err != nil {
				return C_reBreak, err
			}
			fileBinEventHandlingIndex++
			one.EventIdx, one.SqlType, one.Timestamp = fileBinEventHandlingIndex, sqlType, h.Timestamp
			one.TrxIndex, one.TrxStatus = fileTrxIndex, trxStatus
			one.Gtid = currentGTID
			cfg.EventChan <- *one
		}
		if sqlType != "" {
			start := tbMapPos
			if sqlType == "query" {
				start = h.LogPos - h.EventSize
			}
			cfg.StatChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: start, StopPos: h.LogPos, Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
		}
	}
	return C_reBreak, nil
}

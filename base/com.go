package base

import (
	"path/filepath"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"my2sql/dsql"
	toolkits "my2sql/toolkits"
)

type BinEventHandlingIndx struct {
	EventIdx uint64
	lock     sync.RWMutex
	Finished bool
}

var (
	G_HandlingBinEventIndex *BinEventHandlingIndx
)

type MyBinEvent struct {
	MyPos       mysql.Position //this is the end position
	EventIdx    uint64
	BinEvent    *replication.RowsEvent
	StartPos    uint32 // this is the start position
	IfRowsEvent bool
	SqlType     string // insert, update, delete
	Timestamp   uint32
	TrxIndex    uint64
	TrxStatus   int           // 0:begin, 1: commit, 2: rollback, -1: in_progress
	QuerySql    *dsql.SqlInfo // for ddl and binlog which is not row format
	OrgSql      string        // for ddl and binlog which is not row format
}

// ReachedAutoStop 使用下一文件的排他边界，不以候选末文件的末事件位置停止。
func (cfg *ConfCmd) ReachedAutoStop(file string) bool {
	return cfg.AutoStopBeforeFile != "" &&
		(mysql.Position{Name: file, Pos: 4}).Compare(mysql.Position{Name: cfg.AutoStopBeforeFile, Pos: 4}) >= 0
}

func (this *MyBinEvent) CheckBinEvent(cfg *ConfCmd, ev *replication.BinlogEvent, currentBinlog *string) int {
	myPos := mysql.Position{Name: *currentBinlog, Pos: ev.Header.LogPos}

	if ev.Header.EventType == replication.ROTATE_EVENT {
		rotatEvent := ev.Event.(*replication.RotateEvent)
		*currentBinlog = string(rotatEvent.NextLogName)
		this.IfRowsEvent = false
		if cfg.ReachedAutoStop(*currentBinlog) {
			return C_reBreak
		}
		return C_reContinue
	}

	if cfg.ReachedAutoStop(*currentBinlog) {
		return C_reBreak
	}

	if cfg.IfSetStartFilePos {
		cmpRe := myPos.Compare(cfg.StartFilePos)
		if cmpRe <= 0 {
			return C_reContinue
		}
	}

	if cfg.IfSetStopFilePos {
		cmpRe := myPos.Compare(cfg.StopFilePos)
		if cmpRe > 0 {
			return C_reBreak
		}
	}
	//fmt.Println(cfg.StartDatetime, cfg.StopDatetime, header.Timestamp)
	if cfg.IfSetStartDateTime {
		if ev.Header.Timestamp < cfg.StartDatetime {
			return C_reContinue
		}
	}

	if cfg.IfSetStopDateTime {
		if ev.Header.Timestamp >= cfg.StopDatetime {
			return C_reContinue
		}
	}
	if cfg.FilterSqlLen == 0 {
		goto BinEventCheck
	}

	if ev.Header.EventType == replication.WRITE_ROWS_EVENTv1 || ev.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("insert") {
			goto BinEventCheck
		} else {
			return C_reContinue
		}
	}

	if ev.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("update") {
			goto BinEventCheck
		} else {
			return C_reContinue
		}
	}

	if ev.Header.EventType == replication.DELETE_ROWS_EVENTv1 || ev.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("delete") {
			goto BinEventCheck
		} else {
			return C_reContinue
		}
	}

BinEventCheck:
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db := string(wrEvent.Table.Schema)
		tb := string(wrEvent.Table.Table)
		/*if !cfg.IsTargetTable(db, tb) {
			return C_reContinue
		}*/

		if len(cfg.Databases) > 0 {
			if !toolkits.ContainsString(cfg.Databases, db) {
				return C_reContinue
			}
		}
		if len(cfg.Tables) > 0 {
			if !toolkits.ContainsString(cfg.Tables, tb) {
				return C_reContinue
			}
		}

		if len(cfg.IgnoreDatabases) > 0 {
			if toolkits.ContainsString(cfg.IgnoreDatabases, db) {
				return C_reContinue
			}
		}

		if len(cfg.IgnoreTables) > 0 {
			if toolkits.ContainsString(cfg.IgnoreTables, tb) {
				return C_reContinue
			}
		}

		this.BinEvent = wrEvent
		this.IfRowsEvent = true
	case replication.QUERY_EVENT:
		this.IfRowsEvent = false

	case replication.XID_EVENT:
		this.IfRowsEvent = false

	case replication.MARIADB_GTID_EVENT:
		this.IfRowsEvent = false

	default:
		this.IfRowsEvent = false
		return C_reContinue
	}

	return C_reProcess

}

func CheckBinHeaderCondition(cfg *ConfCmd, header *replication.EventHeader, currentBinlog string) int {
	// process: 0, continue: 1, break: 2

	myPos := mysql.Position{Name: currentBinlog, Pos: header.LogPos}
	//fmt.Println(cfg.StartFilePos, cfg.IfSetStopFilePos, myPos)
	if cfg.IfSetStartFilePos {
		cmpRe := myPos.Compare(cfg.StartFilePos)
		if cmpRe <= 0 {
			return C_reContinue
		}
	}

	if cfg.IfSetStopFilePos {
		cmpRe := myPos.Compare(cfg.StopFilePos)
		if cmpRe > 0 {
			return C_reBreak
		}
	}

	//fmt.Println(cfg.StartDatetime, cfg.StopDatetime, header.Timestamp)
	if cfg.IfSetStartDateTime {

		if header.Timestamp < cfg.StartDatetime {
			return C_reContinue
		}
	}

	if cfg.IfSetStopDateTime {
		if header.Timestamp >= cfg.StopDatetime {
			return C_reContinue
		}
	}
	if cfg.FilterSqlLen == 0 {
		return C_reProcess
	}

	if header.EventType == replication.WRITE_ROWS_EVENTv1 || header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("insert") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	if header.EventType == replication.UPDATE_ROWS_EVENTv1 || header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("update") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	if header.EventType == replication.DELETE_ROWS_EVENTv1 || header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("delete") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	return C_reProcess
}

func GetFirstBinlogPosToParse(cfg *ConfCmd) (string, int64) {
	var binlog string
	var pos int64
	if cfg.StartFile != "" {
		binlog = filepath.Join(cfg.BinlogDir, cfg.StartFile)
	} else {
		binlog = cfg.GivenBinlogFile
	}
	if cfg.StartPos != 0 {
		pos = int64(cfg.StartPos)
	} else {
		pos = 4
	}

	return binlog, pos
}

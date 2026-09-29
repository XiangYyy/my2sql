package base

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// Synthetic events and heartbeats cannot prove consumption of a physical end offset.
func physicalEvent(h *replication.EventHeader) bool {
	return h.LogPos != 0 && h.Flags&0x20 == 0 && h.EventType != replication.HEARTBEAT_EVENT &&
		!(h.EventType == replication.ROTATE_EVENT && h.Timestamp == 0)
}

func eventAtStop(cfg *ConfCmd, file string, h *replication.EventHeader) (bool, error) {
	if !cfg.IfSetStopFilePos {
		return false, fmt.Errorf("没有物理结束边界，无法证明解析完整")
	}
	// Compare before applying ROTATE: its LogPos belongs to the OLD file.
	pos := mysql.Position{Name: file, Pos: h.LogPos}
	if (mysql.Position{Name: file, Pos: 4}).Compare(cfg.StopFilePos) > 0 {
		return false, fmt.Errorf("尚未读到边界 %s 已进入 %s", cfg.StopFilePos, file)
	}
	if !physicalEvent(h) {
		return false, nil
	}
	return positionAtStop(cfg, pos)
}

func positionAtStop(cfg *ConfCmd, pos mysql.Position) (bool, error) {
	if !cfg.IfSetStopFilePos {
		return false, fmt.Errorf("没有物理结束边界")
	}
	switch pos.Compare(cfg.StopFilePos) {
	case 1:
		return false, fmt.Errorf("事件 %s 越过物理边界 %s，边界并非已读取的事件末尾", pos, cfg.StopFilePos)
	case 0:
		return true, nil
	}
	return false, nil
}

// Called only by the actual replication parser, never by work-type=binlogs.
func (cfg *ConfCmd) prepareReplBoundary(ctx context.Context) error {
	if cfg.WorkType == "binlogs" {
		return fmt.Errorf("binlogs 查询不能启动解析")
	}
	if !cfg.IfSetStopFilePos {
		if cfg.FromDB == nil {
			return fmt.Errorf("获取物理快照需要数据库连接")
		}
		ctx, cancel := context.WithTimeout(ctx, EventTimeout)
		defer cancel()
		rows, err := cfg.FromDB.QueryContext(ctx, "SHOW BINARY LOG STATUS")
		if serverErr, ok := err.(*mysqldriver.MySQLError); ok && serverErr.Number == 1064 {
			rows, err = cfg.FromDB.QueryContext(ctx, "SHOW MASTER STATUS")
		}
		if err != nil {
			return fmt.Errorf("读取 binlog 快照失败: %w", err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("源库没有返回 binlog 快照")
		}
		data := make([]string, len(cols))
		args := make([]interface{}, len(cols))
		for i := range data {
			args[i] = &data[i]
		}
		if err := rows.Scan(args...); err != nil {
			return err
		}
		var file string
		var pos uint64
		for i, col := range cols {
			switch strings.ToLower(col) {
			case "file":
				file = data[i]
			case "position":
				pos, err = strconv.ParseUint(data[i], 10, 32)
				if err != nil {
					return err
				}
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if file == "" || pos < 4 {
			return fmt.Errorf("无效的 binlog 快照 %q:%d", file, pos)
		}
		cfg.StopFile, cfg.StopPos = file, uint(pos)
		cfg.StopFilePos = mysql.Position{Name: file, Pos: uint32(pos)}
		cfg.IfSetStopFilePos = true
	}
	if cfg.StartFile == "" {
		if cfg.FromDB == nil {
			return fmt.Errorf("没有起始文件")
		}
		files, err := (RemoteBinlogSource{Config: cfg}).List(ctx)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			return fmt.Errorf("没有可读取的 binlog 文件")
		}
		cfg.StartFile = files[0].Name
	}
	if cfg.StartPos < 4 {
		cfg.StartPos = 4
	}
	cfg.StartFile = filepath.Base(cfg.StartFile)
	cfg.StartFilePos = mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	cfg.IfSetStartFilePos = true
	cfg.AutoStopBeforeFile = ""
	if cfg.StartFilePos.Compare(cfg.StopFilePos) > 0 {
		return fmt.Errorf("起点晚于源快照边界")
	}
	return nil
}

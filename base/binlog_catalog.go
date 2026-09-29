package base

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const binlogProbeEventLimit = 64

// BinlogInfo 中的时间是文件头部业务事件的样本，不是文件的精确最小时间。
type BinlogInfo struct {
	Name       string
	Size       uint64
	SampleTime uint32
	Readable   bool
	Status     string
	ProbeError error
}

// BinlogSource 允许目录查询和事件探测在测试中被替换。
type BinlogSource interface {
	List(context.Context) ([]BinlogInfo, error)
	Probe(context.Context, BinlogInfo) BinlogInfo
}

type RemoteBinlogSource struct {
	Config *ConfCmd
}

func (s RemoteBinlogSource) List(ctx context.Context) ([]BinlogInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, EventTimeout)
	defer cancel()
	rows, err := s.Config.FromDB.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("查询 binlog 列表失败，请检查 log_bin 及 REPLICATION CLIENT 权限: %w", err)
	}
	defer rows.Close()
	return readBinlogList(rows)
}

func readBinlogList(rows *sql.Rows) ([]BinlogInfo, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	nameIndex, sizeIndex := -1, -1
	for i, col := range columns {
		switch strings.ToLower(col) {
		case "log_name":
			nameIndex = i
		case "file_size":
			sizeIndex = i
		}
	}
	if nameIndex < 0 || sizeIndex < 0 {
		return nil, fmt.Errorf("binlog 列表缺少 Log_name 或 File_size 列")
	}
	var result []BinlogInfo
	for rows.Next() {
		data := make([]sql.RawBytes, len(columns))
		values := make([]interface{}, len(columns))
		for i := range data {
			values[i] = &data[i]
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		size, err := strconv.ParseUint(string(data[sizeIndex]), 10, 64)
		if err != nil || len(data[nameIndex]) == 0 {
			return nil, fmt.Errorf("无效的 binlog 列表行: 文件名=%q, 大小=%q", data[nameIndex], data[sizeIndex])
		}
		result = append(result, BinlogInfo{Name: string(data[nameIndex]), Size: size})
	}
	return result, rows.Err()
}

func (s RemoteBinlogSource) Probe(ctx context.Context, file BinlogInfo) BinlogInfo {
	ctx, cancel := context.WithTimeout(ctx, EventTimeout)
	defer cancel()
	_, stream, closeStream, err := startBinlogStream(ctx, s.Config, mysql.Position{Name: file.Name, Pos: 4}, true)
	if err != nil {
		file.Status, file.ProbeError = "error", err
		return file
	}
	defer closeStream()
	return probeBinlogEvents(ctx, file, stream)
}

type binlogEventReader interface {
	GetEvent(context.Context) (*replication.BinlogEvent, error)
}

func probeBinlogEvents(ctx context.Context, file BinlogInfo, stream binlogEventReader) BinlogInfo {
	file.SampleTime, file.Readable, file.ProbeError = 0, false, nil
	file.Status = "unknown"
	businessSeen := false
	for count := 0; count < binlogProbeEventLimit; count++ {
		ev, err := stream.GetEvent(ctx)
		if err != nil {
			file.Status, file.ProbeError = "error", err
			return file
		}
		if ev == nil || ev.Header == nil {
			file.Status, file.ProbeError = "error", fmt.Errorf("探测收到空事件")
			return file
		}
		h := ev.Header
		if h.EventType == replication.ROTATE_EVENT {
			rotate, ok := ev.Event.(*replication.RotateEvent)
			if !ok {
				file.Status, file.ProbeError = "error", fmt.Errorf("无效的 ROTATE 事件")
				return file
			}
			if string(rotate.NextLogName) != file.Name {
				return finishBinlogProbe(file, businessSeen)
			}
			continue
		}
		if h.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			file.Readable = true
		}
		// 合成文件头的 LogPos 为零；新写入且超过目录快照的事件不属于本次探测。
		if h.LogPos > 0 && uint64(h.LogPos) > file.Size {
			return finishBinlogProbe(file, businessSeen)
		}
		if isBinlogBusinessEvent(h.EventType) {
			businessSeen = true
		}
		if h.Timestamp != 0 && isBinlogBusinessEvent(h.EventType) {
			if !file.Readable {
				file.Status, file.ProbeError = "error", fmt.Errorf("业务事件之前未读取到文件格式描述")
				return file
			}
			file.SampleTime, file.Status = h.Timestamp, "sampled"
			return file
		}
		if h.LogPos > 0 && uint64(h.LogPos) >= file.Size {
			return finishBinlogProbe(file, businessSeen)
		}
	}
	file.Status = "probe-limit"
	return file
}

func finishBinlogProbe(file BinlogInfo, businessSeen bool) BinlogInfo {
	if file.Readable {
		file.Status = "header-only"
		if businessSeen {
			file.Status = "unknown"
		}
	} else {
		file.Status, file.ProbeError = "error", fmt.Errorf("文件结束前未读取到有效的格式描述")
	}
	return file
}

func isBinlogBusinessEvent(t replication.EventType) bool {
	switch t {
	case replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, replication.XID_EVENT,
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return true
	}
	return false
}

// LoadBinlogCatalog 在探测后重新验证保留文件，遇到 PURGE 至多重做一次。
func LoadBinlogCatalog(ctx context.Context, source BinlogSource) ([]BinlogInfo, error) {
	files, err := source.List(ctx)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		if len(files) == 0 {
			return files, nil
		}
		catalog := make([]BinlogInfo, len(files))
		for i, file := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			catalog[i] = source.Probe(ctx, file)
		}
		current, err := source.List(ctx)
		if err != nil {
			return nil, err
		}
		retained := make(map[string]uint64, len(current))
		for _, file := range current {
			retained[file.Name] = file.Size
		}
		changed := false
		for _, file := range files {
			if size, ok := retained[file.Name]; !ok || size < file.Size {
				changed = true
				break
			}
		}
		if !changed {
			return catalog, nil
		}
		files = current
	}
	return nil, fmt.Errorf("binlog 保留列表连续发生变化（可能正在 PURGE），请稍后重试")
}

type BinlogSelection struct {
	First      int
	Last       int
	StopBefore string
	Warnings   []string
}

func SelectBinlogs(files []BinlogInfo, cfg *ConfCmd) BinlogSelection {
	s := BinlogSelection{First: 0, Last: len(files) - 1}
	if len(files) == 0 || (!cfg.IfSetStartDateTime && !cfg.IfSetStopDateTime) {
		return s
	}
	for i, file := range files {
		if file.SampleTime == 0 || !file.Readable || file.ProbeError != nil {
			s.Warnings = append(s.Warnings, "存在未知时间或探测失败，保守保留全部文件")
			return s
		}
		if i > 0 && file.SampleTime < files[i-1].SampleTime {
			s.Warnings = append(s.Warnings, "样本时间倒序，保守保留全部文件")
			return s
		}
	}
	if (cfg.IfSetStartDateTime && cfg.StartDatetime < files[0].SampleTime) ||
		(cfg.IfSetStopDateTime && cfg.StopDatetime < files[0].SampleTime) {
		s.Warnings = append(s.Warnings, "请求早于最早样本；更早日志可能已清理，样本不是精确最早时间")
	}
	if cfg.IfSetStartDateTime && cfg.StartDatetime > files[len(files)-1].SampleTime {
		s.Warnings = append(s.Warnings, "请求晚于最新样本，保留末尾候选，不能据此断言没有记录")
	}
	first, last := -1, -1
	for i, file := range files {
		// 首文件向过去开放，末文件向未来开放；相等边界也保守纳入。
		afterStart := !cfg.IfSetStartDateTime || i == len(files)-1 || files[i+1].SampleTime >= cfg.StartDatetime
		beforeStop := !cfg.IfSetStopDateTime || i == 0 || file.SampleTime <= cfg.StopDatetime
		if afterStart && beforeStop {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		s.Warnings = append(s.Warnings, "无法界定时间交集，保守保留全部文件")
		return s
	}
	if first > 0 {
		first--
	}
	if last < len(files)-1 {
		last++
	}
	for first > 0 && files[first-1].SampleTime == files[first].SampleTime {
		first--
	}
	for last < len(files)-1 && files[last+1].SampleTime == files[last].SampleTime {
		last++
	}
	s.First, s.Last = first, last
	if last+1 < len(files) {
		s.StopBefore = files[last+1].Name
	}
	return s
}

func (cfg *ConfCmd) ShouldAutoPosition() bool {
	return cfg.AutoPosition && cfg.Mode == "repl" && cfg.MysqlType == "mysql" &&
		cfg.WorkType != "binlogs" && !cfg.ExplicitFilePos && cfg.StartFile == "" && cfg.StopFile == "" &&
		(cfg.IfSetStartDateTime || cfg.IfSetStopDateTime)
}

// DiscoverBinlogs 返回是否继续初始化正式解析资源；查询模式始终提前返回。
func DiscoverBinlogs(ctx context.Context, cfg *ConfCmd, source BinlogSource, out, diagnostics io.Writer) (bool, error) {
	if cfg.WorkType != "binlogs" && !cfg.ShouldAutoPosition() {
		return true, nil
	}
	files, err := LoadBinlogCatalog(ctx, source)
	if err != nil {
		return false, err
	}
	s := SelectBinlogs(files, cfg)
	for _, warning := range s.Warnings {
		fmt.Fprintln(diagnostics, "警告:", warning)
	}
	var failures int
	for _, file := range files {
		if file.ProbeError != nil {
			failures++
			fmt.Fprintf(diagnostics, "探测 %s 失败: %v\n", file.Name, file.ProbeError)
		}
	}
	if cfg.WorkType == "binlogs" {
		if err := PrintBinlogCatalog(out, files, s, GBinlogTimeLocation); err != nil {
			return false, err
		}
		if failures > 0 {
			return false, fmt.Errorf("%d 个文件探测失败，查询结果不完整", failures)
		}
		return false, nil
	}
	if len(files) == 0 {
		fmt.Fprintln(diagnostics, "无可用 binlog 文件")
		return false, nil
	}
	// Sample times cannot safely exclude files with non-monotonic event timestamps.
	cfg.StartFile, cfg.StartPos = files[0].Name, 4
	cfg.StartFilePos = mysql.Position{Name: cfg.StartFile, Pos: 4}
	cfg.IfSetStartFilePos = true
	cfg.AutoStopBeforeFile = ""
	fmt.Fprintf(diagnostics, "自动时间定位（启发式）候选 %s 至 %s；实际解析从 %s:4 到入口处源快照，逐事件过滤，避免时间乱序漏事件。\n", files[s.First].Name, files[s.Last].Name, cfg.StartFile)
	return true, nil
}

func PrintBinlogCatalog(out io.Writer, files []BinlogInfo, s BinlogSelection, location *time.Location) error {
	var text strings.Builder
	if len(files) == 0 {
		_, err := io.WriteString(out, "无可用 binlog 文件\n")
		return err
	}
	if location == nil {
		location = time.Local
	}
	formatTime := func(timestamp uint32) string {
		if timestamp == 0 {
			return "unknown"
		}
		return time.Unix(int64(timestamp), 0).In(location).Format("2006-01-02 15:04:05")
	}
	fmt.Fprintf(&text, "最早保留文件: %s\n", files[0].Name)
	readable := "unknown"
	for _, file := range files {
		if file.Readable {
			readable = file.Name
			break
		}
	}
	fmt.Fprintf(&text, "最早探测可读文件: %s（不保证历史表结构齐全或可生成回滚 SQL）\n", readable)
	fmt.Fprintf(&text, "时间为抽样/估算，时区: %s；不是精确首末事件时间，不保证覆盖所有乱序事件。\n", location)
	table := tabwriter.NewWriter(&text, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "File\tSizeBytes\tSampleStartTime\tEstimatedEndTime\tStatus")
	for i := s.First; i <= s.Last; i++ {
		file := files[i]
		end := "unknown/open"
		if i+1 < len(files) {
			end = "unknown"
			if file.SampleTime > 0 && files[i+1].SampleTime >= file.SampleTime {
				end = formatTime(files[i+1].SampleTime)
			}
		}
		status := file.Status
		if file.ProbeError != nil {
			status += ": " + strings.Join(strings.Fields(file.ProbeError.Error()), " ")
		}
		fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%s\n", file.Name, file.Size, formatTime(file.SampleTime), end, status)
	}
	if err := table.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(&text, "建议解析范围: %s:4 至 %s（含）；仍须带原时间条件。\n", files[s.First].Name, files[s.Last].Name)
	if s.StopBefore != "" {
		fmt.Fprintf(&text, "文件排他终点: -stop-file %s -stop-pos 4\n", s.StopBefore)
	} else {
		fmt.Fprintln(&text, "文件终点: 实际解析入口固定源快照位置；读取超时按不完整失败处理。")
	}
	_, err := io.WriteString(out, text.String())
	return err
}

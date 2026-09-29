package base

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func parseTestOptions(t *testing.T, args ...string) *ConfCmd {
	t.Helper()
	oldFlags, oldArgs, oldUsage, oldLocation := flag.CommandLine, os.Args, flag.Usage, GBinlogTimeLocation
	t.Cleanup(func() {
		flag.CommandLine, os.Args, flag.Usage, GBinlogTimeLocation = oldFlags, oldArgs, oldUsage, oldLocation
	})
	flag.CommandLine = flag.NewFlagSet("my2sql-test", flag.ContinueOnError)
	os.Args = append([]string{"my2sql-test", "-password=", "-tl=UTC"}, args...)
	cfg := &ConfCmd{}
	cfg.ParseCmdOptions()
	return cfg
}

func TestQueryDoesNotInitializeOutput(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "不存在的目录", true: "已有结果文件"}[existing], func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "result")
			sentinel := []byte("已有结果，不得截断\n")
			if existing {
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"binlog_status.txt", "biglong_trx.txt"} {
					if err := os.WriteFile(filepath.Join(dir, name), sentinel, 0644); err != nil {
						t.Fatal(err)
					}
				}
			}
			cfg := parseTestOptions(t, "-work-type=binlogs", "-output-dir="+dir)
			defer cfg.CloseResources()
			if err := cfg.InitOutput(); err != nil {
				t.Fatal(err)
			}
			if cfg.FromDB != nil || cfg.StatFH != nil || cfg.BiglongFH != nil || cfg.EventChan != nil || cfg.StatChan != nil || cfg.SqlChan != nil || cfg.CloseReplication != nil {
				t.Fatal("查询初始化了正式解析资源")
			}
			if !existing {
				if _, err := os.Stat(dir); !os.IsNotExist(err) {
					t.Fatalf("查询创建了输出目录: %v", err)
				}
			} else {
				for _, name := range []string{"binlog_status.txt", "biglong_trx.txt"} {
					data, err := os.ReadFile(filepath.Join(dir, name))
					if err != nil || !bytes.Equal(data, sentinel) {
						t.Fatalf("查询修改了 %s: %v", name, err)
					}
				}
			}
		})
	}
}

func TestAutoPositionOptions(t *testing.T) {
	for _, tc := range []struct {
		name           string
		args           []string
		auto, explicit bool
	}{
		{"默认开启", nil, true, false},
		{"显式默认起点", []string{"-start-pos=4"}, false, true},
		{"显式默认终点", []string{"-stop-pos=4"}, false, true},
		{"显式空文件", []string{"-start-file="}, false, true},
		{"指定起始文件", []string{"-start-file=mysql-bin.000001"}, false, true},
		{"指定结束文件", []string{"-stop-file=mysql-bin.000005"}, false, true},
		{"关闭优化", []string{"-auto-position=false"}, false, false},
		{"MariaDB", []string{"-mysql-type=mariadb"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-start-datetime=2026-09-01 10:00:00"}, tc.args...)
			cfg := parseTestOptions(t, args...)
			if cfg.ShouldAutoPosition() != tc.auto || cfg.ExplicitFilePos != tc.explicit {
				t.Fatalf("自动定位参数不符: %+v", cfg)
			}
			if cfg.StartPos != 4 || !cfg.IfSetStartDateTime || cfg.BinlogTimeLocation != "UTC" {
				t.Fatal("默认位置或时间条件错误")
			}
		})
	}
	t.Run("无时间", func(t *testing.T) {
		if parseTestOptions(t).ShouldAutoPosition() {
			t.Fatal("无时间条件不应自动定位")
		}
	})
	t.Run("本地文件", func(t *testing.T) {
		cfg := timeRange(100, 200)
		cfg.Mode = "file"
		if cfg.ShouldAutoPosition() {
			t.Fatal("本地文件不应自动定位")
		}
	})
	t.Run("仅结束时间", func(t *testing.T) {
		cfg := parseTestOptions(t, "-stop-datetime=2026-09-01 11:00:00")
		if !cfg.ShouldAutoPosition() || !cfg.IfSetStopDateTime {
			t.Fatal("未处理单侧时间")
		}
	})
}

func TestValidateBinlogOptions(t *testing.T) {
	for _, mutate := range []func(*ConfCmd){
		func(c *ConfCmd) { c.Mode = "file" },
		func(c *ConfCmd) { c.MysqlType = "mariadb" },
		func(c *ConfCmd) { c.StartFile = "mysql-bin.000001" },
		func(c *ConfCmd) { c.StopFile = "mysql-bin.000005" },
		func(c *ConfCmd) { c.ExplicitFilePos = true },
		func(c *ConfCmd) { c.LocalBinFile = "mysql-bin.000001" },
		func(c *ConfCmd) { c.ServerId = 0 },
	} {
		cfg := &ConfCmd{Mode: "repl", MysqlType: "mysql", WorkType: "binlogs", ServerId: 123}
		if err := cfg.ValidateBinlogOptions(); err != nil {
			t.Fatal(err)
		}
		mutate(cfg)
		if err := cfg.ValidateBinlogOptions(); err == nil {
			t.Fatalf("未拒绝非法参数: %+v", cfg)
		}
	}
	cfg := timeRange(100, 200)
	if err := cfg.ValidateBinlogOptions(); err == nil {
		t.Fatal("自动定位未校验 server-id=0")
	}
	cfg.AutoPosition = false
	if err := cfg.ValidateBinlogOptions(); err != nil {
		t.Fatal("关闭自动定位不应影响旧参数行为")
	}
}

func TestOutputInitializationAndCleanup(t *testing.T) {
	cfg := &ConfCmd{WorkType: "stats", OutputDir: filepath.Join(t.TempDir(), "result"), Threads: 2}
	defer cfg.CloseResources()
	if err := cfg.InitOutput(); err != nil {
		t.Fatal(err)
	}
	if cfg.StatFH == nil || cfg.BiglongFH == nil || cfg.EventChan == nil || cfg.StatChan == nil || cfg.SqlChan == nil {
		t.Fatal("正式解析资源未初始化")
	}
	closed := false
	cfg.CloseReplication = func() { closed = true }
	cfg.CloseResources()
	if !closed {
		t.Fatal("未调用复制流清理")
	}
	for _, file := range []*os.File{cfg.StatFH, cfg.BiglongFH} {
		if _, err := file.WriteString("test"); err == nil {
			t.Fatal("文件句柄未关闭")
		}
		info, err := os.Stat(file.Name())
		if err != nil || info.Size() == 0 {
			t.Fatalf("缺少输出文件头: %v", err)
		}
	}
}

func TestOutputInitializationFailureClosesFirstFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "biglong_trx.txt"), 0755); err != nil {
		t.Fatal(err)
	}
	cfg := &ConfCmd{WorkType: "stats", OutputDir: dir, Threads: 2}
	defer cfg.CloseResources()
	if err := cfg.InitOutput(); err == nil {
		t.Fatal("预期第二个输出文件创建失败")
	}
	if cfg.StatFH == nil {
		t.Fatal("未经过首文件初始化")
	}
	if _, err := cfg.StatFH.WriteString("test"); err == nil {
		t.Fatal("初始化失败未关闭首个文件")
	}
}

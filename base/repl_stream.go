package base

import (
	"context"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

// watchedConn 使握手和连接清理也能被取消，而不仅是 GetEvent 等待。
type watchedConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func (c *watchedConn) Close() error {
	var err error
	c.once.Do(func() {
		close(c.done)
		err = c.Conn.Close()
	})
	return err
}

func watchBinlogConnection(ctx context.Context, conn net.Conn) net.Conn {
	c := &watchedConn{Conn: conn, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			c.Close()
		case <-c.done:
		}
	}()
	return c
}

func buildBinlogSyncerConfig(ctx context.Context, cfg *ConfCmd, probe bool) replication.BinlogSyncerConfig {
	handler, _ := log.NewStreamHandler(os.Stderr)
	logger := log.NewDefault(handler)
	if probe {
		logger.SetLevel(log.LevelError)
	}
	return replication.BinlogSyncerConfig{
		ServerID: uint32(cfg.ServerId), Flavor: cfg.MysqlType,
		Host: cfg.Host, Port: uint16(cfg.Port), User: cfg.User, Password: cfg.Passwd,
		Charset: "utf8", SemiSyncEnabled: false,
		TimestampStringLocation: GBinlogTimeLocation, ParseTime: false, UseDecimal: false,
		// 禁用重连，避免依赖 Close 持锁等待 retrySync；断连按不完整结果失败。
		RawModeEnabled: probe, DisableRetrySync: true, Logger: logger,
		Dialer: func(dialCtx context.Context, network, address string) (net.Conn, error) {
			if err := dialCtx.Err(); err != nil {
				return nil, err
			}
			dialer := net.Dialer{Timeout: EventTimeout}
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			deadline := time.Now().Add(EventTimeout)
			if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d
			}
			if err := conn.SetDeadline(deadline); err != nil {
				conn.Close()
				return nil, err
			}
			return watchBinlogConnection(ctx, conn), nil
		},
		Option: func(c *client.Conn) error {
			deadline, _ := ctx.Deadline()
			// 正式复制在握手后恢复原有等待行为，探测保留绝对截止时间。
			return c.SetDeadline(deadline)
		},
	}
}

func startBinlogStream(ctx context.Context, cfg *ConfCmd, pos mysql.Position, probe bool) (*replication.BinlogSyncer, *replication.BinlogStreamer, func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	syncer := replication.NewBinlogSyncer(buildBinlogSyncerConfig(ctx, cfg, probe))
	closeStream := func() {
		// 先取消底层连接；依赖 Close 的辅助连接也会快速失败，避免额外握手挂起。
		cancel()
		syncer.Close()
	}
	stream, err := syncer.StartSync(pos)
	if err != nil {
		closeStream()
		return nil, nil, nil, err
	}
	return syncer, stream, closeStream, nil
}

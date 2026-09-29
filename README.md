# my2sql
go版MySQL binlog解析工具，通过解析MySQL binlog ，可以生成原始SQL、回滚SQL、去除主键的INSERT SQL等，也可以生成DML统计信息。类似工具有binlog2sql、MyFlash、my2fback等，本工具基于my2fback、binlog_rollback工具二次开发而来。


# 用途
* 数据快速回滚(闪回)
* 主从切换后新master丢数据的修复
* 从binlog生成标准SQL，带来的衍生功能
* 生成DML统计信息，可以找到哪些表更新的比较频繁
* IO高TPS高， 查出哪些表在频繁更新
* 找出某个时间点数据库是否有大事务或者长事务
* 主从延迟，分析主库执行的SQL语句
* 除了支持常规数据类型，对大部分工具不支持的数据类型做了支持，比如json、blob、text、emoji等数据类型sql生成


# 产品性能对比
binlog2sql当前是业界使用最广泛的MySQL回滚工具，下面对my2sql和binlog2sql做个性能对比。

|                          |my2sql     |binlog2sql|
|---                       |---         |---   |
|1.1G binlog生成回滚SQL      |  1分40秒   |    65分钟  |
|1.1G binlog生成原始SQL      |  1分30秒   |     50分钟|
|1.1G binlog生成表DML统计信息、以及事务统计信息     |   40秒     |不支持|

# 快速开始
### 执行闪回操作具体操作流程
[https://blog.csdn.net/liuhanran/article/details/107426162](https://blog.csdn.net/liuhanran/article/details/107426162)
### 解析binlog生成标准SQL
[https://blog.csdn.net/liuhanran/article/details/107427204](https://blog.csdn.net/liuhanran/article/details/107427204)
### 解析binlog 统计DML、长事务与大事务分析
[https://blog.csdn.net/liuhanran/article/details/107427391](https://blog.csdn.net/liuhanran/article/details/107427391)



# 重要参数说明
-U	
```
优先使用unique key作为where条件，默认false
```

-mode
```
repl: 伪装成从库解析binlog文件，file: 离线解析binlog文件, 默认repl
```
-local-binlog-file
```
当指定-mode=file 参数时，需要指定-local-binlog-file binlog文件相对路径或绝对路径；默认解析至当前文件末尾，指定时间条件或显式结束文件时可连续解析后续文件。
```

-add-extraInfo
```
是否把database/table/datetime/binlogposition/事务标识信息以注释的方式加入生成的每条sql前，仅 -work-type=2sql|rollback 有效，默认false
```
```
# datetime=2020-07-16_10:44:09 database=orchestrator table=cluster_domain_name binlog=mysql-bin.011519 startpos=15552 stoppos=15773 trxindex=3 gtid=3e11fa47-71ca-11e1-9e33-c80aa9429562:23
UPDATE `orchestrator`.`cluster_domain_name` SET `last_registered`='2020-07-16 10:44:09' WHERE `cluster_name`='192.168.1.1:3306'
```
其中：
- `trxindex`：本次解析内的事务序号，遇 BEGIN 从 1 递增，可用于识别哪些 SQL 属于同一事务；序号只在本次解析内有意义，且解析起点落在事务中间时首个不完整事务为 0
- `gtid`：binlog 中记录的真实事务 id（`uuid:gno`）；`gtid_mode=OFF`（匿名事务）时为空。模式差异：远程解析（`-mode=repl`）起点落在事务中间时，因该位置之前的事务头事件不再下发，首个不完整事务的 gtid 为空；本地解析（`-mode=file`）从文件头 Pos 4 读取并在过滤前维护 GTID 上下文，故即使起点落在事务中间，保留的行事件仍带上其真实 gtid。

-big-trx-row-limit n

```
transaction with affected rows greater or equal to this value is considerated as big transaction 
找出满足n条sql的事务，默认500条
```

-databases 、 -tables
```
库及表条件过滤, 以逗号分隔
```

-sql
```
要解析的sql类型，可选参数insert、update、delete，默认全部解析
```

-doNotAddPrifixDb

```
Prefix table name witch database name in sql,ex: insert into db1.tb1 (x1, x1) values (y1, y1)
默认生成insert into db1.tb1 (x1, x1) values (y1, y1)类sql，也可以生成不带库名的sql
```

-file-per-table
```
为每个表生成一个sql文件
```

-full-columns
```
For update sql, include unchanged columns. for update and delete, use all columns to build where condition.
default false, this is, use changed columns to build set part, use primary/unique key to build where condition
生成的sql是否带全列信息，默认false
```
-ignorePrimaryKeyForInsert
```
生成的insert语句是否去掉主键，默认false
```

-output-dir
```
将生成的结果存放到制定目录
```

-output-toScreen
```
将生成的结果打印到屏幕，默认写到文件
```

-threads
```
线程数，默认8个
```

-work-type
```
2sql：生成原始sql，rollback：生成回滚sql，stats：只统计DML、事务信息，binlogs：独立查询远程 MySQL binlog 文件及估算时间
```















# 使用案例
### 查询远程 MySQL binlog 文件和时间范围
新功能仅适用于 `-mode repl -mysql-type mysql`，需要重新编译当前源码；仓库内预编译二进制不会自动更新。

```sh
# 列出全部保留文件，并显示最早保留文件、最早探测可读文件；省略密码时交互输入
./my2sql -host 127.0.0.1 -user reader -work-type binlogs

# 按时间查找候选文件；也可只传开始时间或结束时间
./my2sql -host 127.0.0.1 -user reader -work-type binlogs -start-datetime "2026-09-01 10:00:00" -stop-datetime "2026-09-01 11:00:00" -tl Asia/Shanghai
```

- 表格列为 `File / SizeBytes / SampleStartTime / EstimatedEndTime / Status`。样本开始时间取文件开头首个时间戳非零的业务事件，估算结束时间取下一文件样本；末文件显示 `unknown/open`，不是当前时间。
- 状态包括 `sampled`（已获得样本）、`header-only`（探测范围内仅有控制事件）、`unknown`（业务时间未知）、`probe-limit`（达到事件上限）、`error`（失败，附原因）。
- “最早探测可读”只表示保留且复制读取/文件头解析成功，不保证历史表结构齐全或一定能生成回滚 SQL。
- 查询只输出到终端，不生成 SQL、统计文件，不启动 SQL worker、不读取表结构；即使传 `-output-dir` 也不会创建目录或截断已有文件。表格写 stdout，诊断和密码提示写 stderr。
- 查询不接受文件/Pos 参数，包括显式 `-start-pos 4`，请使用时间条件。探测失败保留错误行，退出状态非零，表示查询结果不完整。

### 纯时间解析自动定位
`-auto-position` 默认开启：远程 MySQL 的 `2sql`、`rollback`、`stats` 只传时间条件、未显式指定文件/Pos 时，先展示探测候选范围，正式解析仍从最早保留文件 Pos 4 扫描到入口处的源快照，不用样本时间排除文件。

```sh
./my2sql -host 127.0.0.1 -user reader -work-type 2sql -start-datetime "2026-09-01 10:00:00" -stop-datetime "2026-09-01 11:00:00" -tl Asia/Shanghai -output-dir ./tmpdir

# 跳过候选探测，仍从最早保留文件扫描至固定的物理上界
./my2sql -host 127.0.0.1 -user reader -work-type 2sql -start-datetime "2026-09-01 10:00:00" -stop-datetime "2026-09-01 11:00:00" -tl Asia/Shanghai -auto-position=false -output-dir ./tmpdir
```

- 时间区间为 `[start-datetime, stop-datetime)`，输入和查询展示统一使用 `-tl`。显式文件/Pos 优先，不被自动定位覆盖；本地文件和 MariaDB 不自动定位。
- 每个文件串行从 Pos 4 短暂探测，使用 raw 模式，不解析行数据；单文件最多消费 64 个事件、超时 5 秒，忽略 ROTATE、FORMAT_DESCRIPTION、GTID、PREVIOUS_GTIDS、HEARTBEAT 等控制事件。依赖存在异步预读，不保证只传事件头或严格限制网络字节数。
- 目录探测成本为 O(文件数) 次短探测；展示的相交候选前后各保留一个相邻文件，同秒样本组不拆分。正式解析会扫描实际物理范围内的所有事件，其成本不受短探测上限约束。
- 出现未知时间、探测失败或样本倒序时，告警并将候选退回全部保留文件。时间早于最早样本可能意味着更早日志已清理；晚于最新样本不代表没有记录。PURGE 导致目录变化时最多刷新重试一次，连续变化时报错。
- 正式远程解析在未指定物理终点时固定入口处的源库 binlog 文件/位点快照；纯时间请求从最早保留文件开始，候选范围不用于裁剪扫描范围。未到达物理上界的读取空闲超时属于失败，不持续订阅未来日志。
- 样本/估算不是精确首末时间，仅供目录查询参考，不能排除未采样的时间乱序。`-auto-position=false` 可跳过候选探测；显式指定文件/Pos 才能缩小物理扫描范围，该范围之外或已清理的事件不在覆盖保证内。
- 查询需要 `REPLICATION CLIENT`、`REPLICATION SLAVE`（或服务端对应的复制权限）；生成 SQL 仍需表结构读取权限。`-server-id` 必须是非零 uint32 且在复制拓扑中唯一；探测与正式复制不会同时使用同一 ID。不执行轮转、清理或配置变更。

### 解析边界与失败处理
- 时间条件为事件头时间的 `[start-datetime, stop-datetime)`，区间外事件只被过滤，不因先遇到较晚时间戳而终止扫描；不保证事务完整，也不重建历史表结构。
- `stop-pos` 表示真实事件的结束位点，包含结束于该位点且满足筛选条件的事件，处理后立即停止，无需传 `P+1`。即使末事件被筛选掉也会终止；心跳和伪造的复制控制事件不能证明到达边界。越过非事件末尾的停止位点会报错，不静默截断。
- 远程解析未指定物理终点时固定源库当前快照；本地文件解析默认以文件大小为上界，纯时间模式可包含连续的后续本地文件。到达上界前发生超时、提前 EOF、范围内文件缺失或解析错误均非零退出。
- 本地解析指定下一文件 Pos 4（例如 `-stop-file mysql-bin.000002 -stop-pos 4`）表示在该文件开始前停止；不会打开或读取该终点文件，也不要求它存在；该排除只作用于终点文件，起始文件仍会被打开校验。范围内的中间文件缺失或事件截断仍会失败。
- 探测和正式复制均禁用自动重连，避免当前复制依赖在关闭与重连并发时互相等待。正式复制断连且未到物理终点时按不完整结果报错退出，需要人工重新运行；已有部分结果不能当作完整产物，重跑请使用独立输出目录，避免覆盖或混用。目录因 PURGE 变化时的一次刷新重试不受影响。
- SQL、统计文件的打开、写入、刷新、关闭以及回滚逆序错误参与最终退出状态；失败时停止生成有效输出并排空内部通道，不能把部分文件当作成功产物。逆序失败保留对应临时源文件，只有全部阶段成功才输出完成标记。
- 列数不匹配会失败；列数相同的历史列重排等情况仍不能可靠识别，必须使用匹配的历史结构并在隔离库验证结果。

### 解析出标准SQL
#### 根据时间点解析出标准SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl -work-type 2sql  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode file -local-binlog-file ./mysql-bin.011259  -work-type 2sql  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
```

#### 根据pos点解析出标准SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode file -local-binlog-file ./mysql-bin.011259  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir
```

### 解析出回滚SQL
#### 根据时间点解析出回滚SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl -work-type rollback  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode file -local-binlog-file ./mysql-bin.011259 -work-type rollback  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
```

#### 根据pos点解析出回滚SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl -work-type rollback  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306   -mode file -local-binlog-file ./mysql-bin.011259  -work-type rollback  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir

```

### 统计DML以及大事务
#### 统计时间范围各个表的DML操作数量，统计一个事务大于500条、时间大于300秒的事务
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode repl -work-type stats  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00"  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode file -local-binlog-file ./mysql-bin.011259   -work-type stats  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00"  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
```

#### 统计一段pos点范围各个表的DML操作数量，统计一个事务大于500条、时间大于300秒的事务
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode repl -work-type stats  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode file -local-binlog-file ./mysql-bin.011259  -work-type stats  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
```


### 从某一个pos点解析到入口处源快照，并打印到屏幕
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4   -output-toScreen 
```

# 下载二进制版本
 + 有编译好的linux版本(CentOS release 7.x)  [点击下载Linux版](https://github.com/liuhr/my2sql/blob/master/releases/centOS_release_7.x/my2sql)

# 编译安装
```
cd $GOPATH/src
git clone https://github.com/liuhr/my2sql.git
cd my2sql/
go build .
```


# 限制
* 使用回滚/闪回功能时，binlog格式必须为row,且binlog_row_image=full， DML统计以及大事务分析不受影响
* 只能回滚DML， 不能回滚DDL
* 使用rollback功能时，要解析的binlog段，表结构要保持一致（例如：解析mysql-bin.000001文件，此binlog文件的的表有add column或drop column操作，则执行rollback可能会执行异常）
* 支持指定-tl时区来解释binlog中time/datetime字段的内容。开始时间-start-datetime与结束时间-stop-datetime也会使用此指定的时区，
  但注意此开始与结束时间针对的是binlog event header中保存的unix timestamp。结果中的额外的datetime时间信息都是binlog event header中的unix
timestamp
* 此工具是伪装成从库拉取binlog，需要连接数据库的用户有SELECT, REPLICATION SLAVE, REPLICATION CLIENT权限
* MySQL8.0版本需要在配置文件中加入default_authentication_plugin  =mysql_native_password，用户密码认证必须是mysql_native_password才能解析

# 感谢
 感谢[https://github.com/siddontang](https://github.com/siddontang)的binlog解析库， 感谢dropbox的sqlbuilder库，感谢my2fback、binlog_rollback

# TODO
- [x] GTID事务为单位进行解析
- [x] 闪回、回滚添加begin/commit事务标示

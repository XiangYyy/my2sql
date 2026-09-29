package base

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	constvar "my2sql/constvar"
	toolkits "my2sql/toolkits"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"golang.org/x/term"
)

const (
	C_Version      = "my2sql V2.0"
	C_validOptMsg  = "valid options are: "
	C_joinSepComma = ","

	EventTimeout = 5 * time.Second

	C_unknownColPrefix   = "dropped_column_"
	C_unknownColType     = "unknown_type"
	C_unknownColTypeCode = mysql.MYSQL_TYPE_NULL

	C_trxBegin    = 0
	C_trxCommit   = 1
	C_trxRollback = 2
	C_trxProcess  = -1

	C_reProcess  = 0
	C_reContinue = 1
	C_reBreak    = 2
	C_reFileEnd  = 3
)

var (
	GConfCmd            *ConfCmd = &ConfCmd{}
	GBinlogTimeLocation *time.Location
	//GSqlParser          *parser.Parser = parser.New()

	GUseDatabase string = ""

	GOptsValidMode      []string = []string{"repl", "file"}
	GOptsValidWorkType  []string = []string{"2sql", "rollback", "stats", "binlogs"}
	GOptsValidMysqlType []string = []string{"mysql", "mariadb"}
	GOptsValidFilterSql []string = []string{"insert", "update", "delete"}

	GOptsValueRange map[string][]int = map[string][]int{
		"PrintInterval":  []int{1, 600, 30},
		"BigTrxRowLimit": []int{1, 30000, 10},
		"LongTrxSeconds": []int{0, 3600, 1},
		"InsertRows":     []int{1, 500, 30},
		"Threads":        []int{1, 16, 2},
	}

	GStatsColumns []string = []string{
		"StartTime", "StopTime", "Binlog", "PosRange",
		"Database", "Table",
		"BigTrxs", "BiggestTrx", "LongTrxs", "LongestTrx",
		"Inserts", "Updates", "Deletes", "Trxs", "Statements",
		"Renames", "RenamePoses", "Ddls", "DdlPoses",
	}

	GDdlPrintHeader []string = []string{"datetime", "binlog", "startposition", "stopposition", "sql"}
	//GThreadsFinished          = &Threads_Finish_Status{finishedThreadsCnt: 0, threadsCnt: 0}
)

type ConfCmd struct {
	Mode      string
	WorkType  string
	MysqlType string

	Host   string
	Port   uint
	User   string
	Passwd string
	// AskPasswd bool
	ServerId uint

	Databases []string
	Tables    []string
	//DatabaseRegs []*regexp.Regexp
	//ifHasDbReg   bool
	//TableRegs    []*regexp.Regexp
	//ifHasTbReg   bool
	IgnoreDatabases []string
	IgnoreTables    []string
	FilterSql       []string
	FilterSqlLen    int

	AutoPosition       bool
	ExplicitFilePos    bool
	AutoStopBeforeFile string

	StartFile         string
	StartPos          uint
	StartFilePos      mysql.Position
	IfSetStartFilePos bool

	StopFile         string
	StopPos          uint
	StopFilePos      mysql.Position
	IfSetStopFilePos bool

	StartDatetime      uint32
	StopDatetime       uint32
	BinlogTimeLocation string

	IfSetStartDateTime bool
	IfSetStopDateTime  bool

	LocalBinFile string

	OutputToScreen bool
	PrintInterval  int
	BigTrxRowLimit int
	LongTrxSeconds int

	IfSetStopParsPoint bool

	OutputDir string

	//MinColumns     bool
	FullColumns    bool
	InsertRows     int
	KeepTrx        bool
	SqlTblPrefixDb bool
	FilePerTable   bool

	PrintExtraInfo bool

	Threads uint

	ReadTblDefJsonFile string
	OnlyColFromFile    bool
	DumpTblDefToFile   string

	BinlogDir string

	GivenBinlogFile string

	UseUniqueKeyFirst         bool
	IgnorePrimaryKeyForInsert bool
	ReplaceIntoForInsert      bool

	//DdlRegexp string
	ParseStatementSql bool

	IgnoreParsedErrForSql string // if parsed error, for sql match this regexp, only print error info, but not exits
	IgnoreParsedErrRegexp *regexp.Regexp

	EventChan  chan MyBinEvent
	StatChan   chan BinEventStats
	OrgSqlChan chan OrgSqlPrint
	SqlChan    chan ForwardRollbackSqlOfPrint

	StatFH *os.File
	//DdlFH     *os.File
	BiglongFH *os.File

	BinlogStreamer   *replication.BinlogStreamer
	CloseReplication func()
	FromDB           *sql.DB

	resultMu    sync.Mutex
	firstError  error
	statsClosed bool
	// Optional output factory for offline fault injection.
	openOutput func(string) (io.WriteCloser, error)
}

func (this *ConfCmd) ParseCmdOptions() {
	var (
		version   bool
		dbs       string
		tbs       string
		ignoreDbs string
		ignoreTbs string

		sqlTypes         string
		startTime        string
		stopTime         string
		err              error
		doNotAddPrifixDb bool
	)

	flag.Usage = func() {
		this.PrintUsageMsg()
	}

	flag.BoolVar(&version, "v", false, "print version")
	flag.StringVar(&this.Mode, "mode", "repl", StrSliceToString(GOptsValidMode, C_joinSepComma, C_validOptMsg)+". repl: as a slave to get binlogs from master. file: get binlogs from local filesystem. default repl")
	flag.StringVar(&this.WorkType, "work-type", "2sql", StrSliceToString(GOptsValidWorkType, C_joinSepComma, C_validOptMsg)+". 2sql: convert binlog to sqls, rollback: generate rollback sqls, stats: analyze transactions, binlogs: 查询远程 MySQL binlog 候选文件及估算时间. default: 2sql")
	flag.StringVar(&this.MysqlType, "mysql-type", "mysql", StrSliceToString(GOptsValidMysqlType, C_joinSepComma, C_validOptMsg)+". server of binlog, mysql or mariadb, default mysql")

	flag.StringVar(&this.Host, "host", "127.0.0.1", "mysql host, default 127.0.0.1 .")
	flag.UintVar(&this.Port, "port", 3306, "mysql port, default 3306.")
	flag.StringVar(&this.User, "user", "", "mysql user. ")
	flag.StringVar(&this.Passwd, "password", "nil", "mysql user password.if not given,asked from the tty.")
	// flag.BoolVar(&this.AskPasswd, "askpass", false, "交互输入密码")
	flag.UintVar(&this.ServerId, "server-id", 1113306, "this program replicates from mysql as slave to read binlogs. Must set this server id unique from other slaves, default 1113306")

	flag.StringVar(&dbs, "databases", "", "only parse these databases, comma seperated, default all.")
	flag.StringVar(&tbs, "tables", "", "only parse these tables, comma seperated, DONOT prefix with schema, default all.")
	flag.StringVar(&ignoreDbs, "ignore-databases", "", "ignore parse these databases, comma seperated, default null")
	flag.StringVar(&ignoreTbs, "ignore-tables", "", "ignore parse these tables, comma seperated, default null")
	flag.StringVar(&sqlTypes, "sql", "", StrSliceToString(GOptsValidFilterSql, C_joinSepComma, C_validOptMsg)+". only parse these types of sql, comma seperated, valid types are: insert, update, delete; default is all(insert,update,delete)")
	flag.BoolVar(&this.IgnorePrimaryKeyForInsert, "ignore-primaryKey-forInsert", false, "for insert statement when -workType=2sql, ignore primary key")

	flag.BoolVar(&this.AutoPosition, "auto-position", true, "纯时间解析时自动探测远程 MySQL 候选文件；抽样不保证覆盖所有乱序事件，设为 false 恢复原路径")
	flag.StringVar(&this.StartFile, "start-file", "", "binlog file to start reading")
	flag.UintVar(&this.StartPos, "start-pos", 4, "start reading the binlog at position")
	flag.StringVar(&this.StopFile, "stop-file", "", "binlog file to stop reading")
	flag.UintVar(&this.StopPos, "stop-pos", 4, "Stop reading the binlog at position")
	flag.StringVar(&this.LocalBinFile, "local-binlog-file", "", "local binlog files to process, It works with -mode=file ")

	flag.StringVar(&this.BinlogTimeLocation, "tl", "Local", "time location to parse timestamp/datetime column in binlog, such as Asia/Shanghai. default Local")
	flag.StringVar(&startTime, "start-datetime", "", "Start reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2020-01-01 01:00:00\"")
	flag.StringVar(&stopTime, "stop-datetime", "", "Stop reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2020-12-30 01:00:00\"")

	flag.BoolVar(&this.OutputToScreen, "output-toScreen", false, "Just output to screen,do not write to file")
	flag.BoolVar(&this.PrintExtraInfo, "add-extraInfo", false, "Works with -work-type=2sql|rollback. Print database/table/datetime/binlogposition...info on the line before sql, default false")

	flag.BoolVar(&this.FullColumns, "full-columns", false, "For update sql, include unchanged columns. for update and delete, use all columns to build where condition.\t\ndefault false, this is, use changed columns to build set part, use primary/unique key to build where condition")
	flag.BoolVar(&doNotAddPrifixDb, "do-not-add-prifixDb", false, "Prefix table name witch database name in sql,ex: insert into db1.tb1 (x1, x1) values (y1, y1). ")
	flag.BoolVar(&this.UseUniqueKeyFirst, "U", false, "prefer to use unique key instead of primary key to build where condition for delete/update sql")

	flag.StringVar(&this.OutputDir, "output-dir", "", "result output dir, default current work dir. Attension, result files could be large, set it to a dir with large free space")
	flag.BoolVar(&this.FilePerTable, "file-per-table", false, "One file for one table if true, else one file for all tables. default false. Attention, always one file for one binlog")
	flag.IntVar(&this.PrintInterval, "print-interval", this.GetDefaultValueOfRange("PrintInterval"), "works with -w='stats', print stats info each PrintInterval. "+this.GetDefaultAndRangeValueMsg("PrintInterval"))
	flag.IntVar(&this.BigTrxRowLimit, "big-trx-row-limit", this.GetDefaultValueOfRange("BigTrxRowLimit"), "transaction with affected rows greater or equal to this value is considerated as big transaction. "+this.GetDefaultAndRangeValueMsg("BigTrxRowLimit"))
	flag.IntVar(&this.LongTrxSeconds, "long-trx-seconds", this.GetDefaultValueOfRange("LongTrxSeconds"), "transaction with duration greater or equal to this value is considerated as long transaction. "+this.GetDefaultAndRangeValueMsg("LongTrxSeconds"))

	flag.UintVar(&this.Threads, "threads", uint(this.GetDefaultValueOfRange("Threads")), "Works with -workType=2sql|rollback. threads to run")

	flag.Parse()
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "start-file", "start-pos", "stop-file", "stop-pos":
			this.ExplicitFilePos = true
		}
	})

	if version {
		fmt.Printf("%s\n", C_Version)
		os.Exit(0)
	}

	if this.Mode != "repl" && this.Mode != "file" {
		log.Fatalf("unsupported mode=%s, valid modes: file, repl", this.Mode)
	}

	if !doNotAddPrifixDb {
		this.SqlTblPrefixDb = true
	} else {
		this.SqlTblPrefixDb = false
	}

	if dbs != "" {
		this.Databases = CommaSeparatedListToArray(dbs)
	}

	if tbs != "" {
		this.Tables = CommaSeparatedListToArray(tbs)

	}

	if ignoreDbs != "" {
		this.IgnoreDatabases = CommaSeparatedListToArray(ignoreDbs)
	}

	if ignoreTbs != "" {
		this.IgnoreTables = CommaSeparatedListToArray(ignoreTbs)
	}

	if sqlTypes != "" {
		this.FilterSql = CommaSeparatedListToArray(sqlTypes)
		for _, oneSqlT := range this.FilterSql {
			CheckElementOfSliceStr(GOptsValidFilterSql, oneSqlT, "invalid sqltypes", true)
		}
		this.FilterSqlLen = len(this.FilterSql)
	} else {
		this.FilterSqlLen = 0
	}

	GBinlogTimeLocation, err = time.LoadLocation(this.BinlogTimeLocation)
	if err != nil {
		log.Fatalf("invalid time location %v"+this.BinlogTimeLocation, err)
	}

	if startTime != "" {
		t, err := time.ParseInLocation(constvar.DATETIME_FORMAT, startTime, GBinlogTimeLocation)
		if err != nil {
			log.Fatalf("invalid start datetime -start-datetime " + startTime)
		}
		if t.Unix() < 0 || t.Unix() > int64(^uint32(0)) {
			log.Fatal("start-datetime 超出 binlog 时间戳范围")
		}
		this.StartDatetime = uint32(t.Unix())
		this.IfSetStartDateTime = true
	} else {
		this.IfSetStartDateTime = false
	}

	if stopTime != "" {
		t, err := time.ParseInLocation(constvar.DATETIME_FORMAT, stopTime, GBinlogTimeLocation)
		if err != nil {
			log.Fatalf("invalid stop datetime -stop-datetime " + stopTime)
		}
		if t.Unix() < 0 || t.Unix() > int64(^uint32(0)) {
			log.Fatal("stop-datetime 超出 binlog 时间戳范围")
		}
		this.StopDatetime = uint32(t.Unix())
		this.IfSetStopDateTime = true
	} else {
		this.IfSetStopDateTime = false
	}

	if startTime != "" && stopTime != "" {
		if this.StartDatetime >= this.StopDatetime {
			log.Fatalf("-start-datetime must be ealier than -stop-datetime")
		}
	}

	if this.StartFile != "" {
		this.IfSetStartFilePos = true
		this.StartFilePos = mysql.Position{Name: this.StartFile, Pos: uint32(this.StartPos)}

	} else {
		this.IfSetStartFilePos = false
	}

	if this.StopFile != "" {
		this.IfSetStopFilePos = true
		this.StopFilePos = mysql.Position{Name: this.StopFile, Pos: uint32(this.StopPos)}
		this.IfSetStopParsPoint = true

	} else {
		this.IfSetStopFilePos = false
		this.IfSetStopParsPoint = false
	}

	if err := this.ValidateBinlogOptions(); err != nil {
		log.Fatal(err)
	}

	if this.Mode == "file" {

		if this.StartFile == "" {
			log.Fatalf("missing binlog file.  -start-file must be specify when -mode=file ")
		}
		this.GivenBinlogFile = this.StartFile
		if !toolkits.IsFile(this.GivenBinlogFile) {
			log.Fatalf("%s doesnot exists nor a file\n", this.GivenBinlogFile)
		} else {
			this.BinlogDir = filepath.Dir(this.GivenBinlogFile)
		}
	}

	if this.Mode == "file" {
		if this.LocalBinFile == "" {
			log.Fatalf("missing binlog file.  -local-binlog-file must be specify when -mode=file ")
		}
		this.GivenBinlogFile = this.LocalBinFile
		if !toolkits.IsFile(this.GivenBinlogFile) {
			log.Fatalf("%s doesnot exists nor a file\n", this.GivenBinlogFile)
		} else {
			this.BinlogDir = filepath.Dir(this.GivenBinlogFile)
		}
	}

	this.CheckCmdOptions()
}

func (this *ConfCmd) ValidateBinlogOptions() error {
	if this.WorkType == "binlogs" {
		if this.Mode != "repl" || this.MysqlType != "mysql" {
			return fmt.Errorf("-work-type binlogs 仅支持 -mode repl -mysql-type mysql")
		}
		if this.ExplicitFilePos || this.StartFile != "" || this.StopFile != "" || this.LocalBinFile != "" {
			return fmt.Errorf("-work-type binlogs 不接受文件或 Pos 参数，请使用起止时间筛选")
		}
	}
	if (this.WorkType == "binlogs" || this.ShouldAutoPosition()) && (this.ServerId == 0 || uint64(this.ServerId) > uint64(^uint32(0))) {
		return fmt.Errorf("-server-id 必须介于 1 和 4294967295，且不得与其他副本重复")
	}
	return nil
}

// InitOutput 只能在查询分流和自动定位成功之后调用。
func (this *ConfCmd) InitOutput() error {
	if this.WorkType == "binlogs" {
		return nil
	}
	if this.OutputDir == "" {
		var err error
		this.OutputDir, err = os.Getwd()
		if err != nil {
			return err
		}
	} else if err := os.MkdirAll(this.OutputDir, os.ModePerm); err != nil {
		return fmt.Errorf("创建输出目录失败: %w", err)
	}
	this.EventChan = make(chan MyBinEvent, this.Threads*2)
	this.StatChan = make(chan BinEventStats, this.Threads*2)
	this.SqlChan = make(chan ForwardRollbackSqlOfPrint, this.Threads*2)
	if err := this.OpenStatsResultFiles(); err != nil {
		return err
	}
	if err := this.OpenTxResultFiles(); err != nil {
		this.CloseFH()
		return err
	}
	return nil
}

func (this *ConfCmd) CheckCmdOptions() {
	//check -mode
	CheckElementOfSliceStr(GOptsValidMode, this.Mode, "invalid arg for -mode", true)

	//check -workType
	CheckElementOfSliceStr(GOptsValidWorkType, this.WorkType, "invalid arg for -workType", true)

	//check -mysqlType
	CheckElementOfSliceStr(GOptsValidMysqlType, this.MysqlType, "invalid arg for -mysqlType", true)

	/*if this.Mode == "repl" {
		//check --user
		this.CheckRequiredOption(this.User, "-u must be set", true)
		//check --password
		this.CheckRequiredOption(this.Passwd, "-p must be set", true)
	}*/

	if this.StartFile != "" {
		this.StartFile = filepath.Base(this.StartFile)
	}
	if this.StopFile != "" {
		this.StopFile = filepath.Base(this.StopFile)
	}

	//check --start-binlog --start-pos --stop-binlog --stop-pos
	if this.StartFile != "" && this.StartPos != 0 && this.StopFile != "" && this.StopPos != 0 {
		cmpRes := CompareBinlogPos(this.StartFile, this.StartPos, this.StopFile, this.StopPos)
		if cmpRes != -1 {
			log.Fatalf("start postion(-start-file -start-pos) must less than stop position(-end-file -end-pos)")
		}
	}

	// check --threads
	if this.Threads != uint(this.GetDefaultValueOfRange("Threads")) {
		this.CheckValueInRange("Threads", int(this.Threads), "value of -threads out of range", true)
	}

	// check --interval
	if this.PrintInterval != this.GetDefaultValueOfRange("PrintInterval") {
		this.CheckValueInRange("PrintInterval", this.PrintInterval, "value of -i out of range", true)
	}

	// check --big-trx-rows
	if this.BigTrxRowLimit != this.GetDefaultValueOfRange("BigTrxRowLimit") {
		this.CheckValueInRange("BigTrxRowLimit", this.BigTrxRowLimit, "value of -b out of range", true)
	}

	// check --long-trx-seconds
	if this.LongTrxSeconds != this.GetDefaultValueOfRange("LongTrxSeconds") {
		this.CheckValueInRange("LongTrxSeconds", this.LongTrxSeconds, "value of -l out of range", true)
	}

	// check --threads
	if this.Threads != uint(this.GetDefaultValueOfRange("Threads")) {
		this.CheckValueInRange("Threads", int(this.Threads), "value of -t out of range", true)
	}

	if this.Passwd == "nil" {
		fmt.Fprint(os.Stderr, "Enter Password:")
		fd := int(os.Stdin.Fd())
		password, err := term.ReadPassword(fd)
		if err != nil {
			log.Fatal(err)
		}
		this.Passwd = string(password)
	}

}

func (this *ConfCmd) CheckRequiredOption(v interface{}, prefix string, ifExt bool) bool {
	// options must set, default value is not suitable
	notOk := false
	switch realVal := v.(type) {
	case string:
		if realVal == "" {
			notOk = true
		}
	case int:
		if realVal == 0 {
			notOk = true
		}
	}
	if notOk {
		log.Fatalf("%s", prefix)
	}
	return true
}

func (this *ConfCmd) CheckValueInRange(opt string, val int, prefix string, ifExt bool) bool {
	valOk := true
	if val < this.GetMinValueOfRange(opt) {
		valOk = false
	} else if val > this.GetMaxValueOfRange(opt) {
		valOk = false
	}

	if !valOk {

		if ifExt {
			log.Fatalf(fmt.Sprintf("%s: %d is specfied, but %s\n", prefix, val, this.GetDefaultAndRangeValueMsg(opt)))
		} else {
			log.Error(fmt.Sprintf("%s: %d is specfied, but %s\n", prefix, val, this.GetDefaultAndRangeValueMsg(opt)))
		}
	}
	return valOk
}

func (this *ConfCmd) GetMinValueOfRange(opt string) int {
	return GOptsValueRange[opt][0]
}

func (this *ConfCmd) GetMaxValueOfRange(opt string) int {
	return GOptsValueRange[opt][1]
}

func (this *ConfCmd) GetDefaultValueOfRange(opt string) int {
	//fmt.Printf("default value of %s: %d\n", opt, gOptsValueRange[opt][2])
	return GOptsValueRange[opt][2]
}

func (this *ConfCmd) GetDefaultAndRangeValueMsg(opt string) string {
	return fmt.Sprintf("Valid values range from %d to %d, default %d",
		this.GetMinValueOfRange(opt),
		this.GetMaxValueOfRange(opt),
		this.GetDefaultValueOfRange(opt),
	)
}

/*func (this *ConfCmd) IsTargetTable(db, tb string) bool {
	dbLower := strings.ToLower(db)
	tbLower := strings.ToLower(tb)
	if this.ifHasDbReg {
		ifMatch := false
		for _, oneReg := range this.DatabaseRegs {
			if oneReg.MatchString(dbLower) {
				ifMatch = true
				break
			}
		}
		if !ifMatch {
			return false
		}
	}

	if this.ifHasTbReg {
		ifMatch := false
		for _, oneReg := range this.TableRegs {
			if oneReg.MatchString(tbLower) {
				ifMatch = true
				break
			}
		}
		if !ifMatch {
			return false
		}
	}
	return true

}*/

func (this *ConfCmd) IsTargetDml(dml string) bool {
	if this.FilterSqlLen < 1 {
		return true
	}
	if toolkits.ContainsString(this.FilterSql, dml) {
		return true
	} else {
		return false
	}
}

func (this *ConfCmd) OpenStatsResultFiles() error {
	statFile := filepath.Join(this.OutputDir, "binlog_status.txt")
	statFH, err := os.OpenFile(statFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("打开统计文件 %s 失败: %w", statFile, err)
	}
	this.StatFH = statFH
	if err = writeOutput(statFH, GetStatsPrintHeaderLine(Stats_Result_Header_Column_names)); err != nil {
		this.RecordError(err)
		this.CloseFH()
		return err
	}
	return nil
}

func (this *ConfCmd) OpenTxResultFiles() error {
	biglongFile := filepath.Join(this.OutputDir, "biglong_trx.txt")
	biglongFH, err := os.OpenFile(biglongFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("打开事务文件 %s 失败: %w", biglongFile, err)
	}
	this.BiglongFH = biglongFH
	if err = writeOutput(biglongFH, GetBigLongTrxPrintHeaderLine(Stats_BigLongTrx_Header_Column_names)); err != nil {
		this.RecordError(err)
		this.CloseFH()
		return err
	}
	return nil
}

func (this *ConfCmd) CloseFH() {
	if this.statsClosed {
		return
	}
	this.statsClosed = true
	if this.StatFH != nil {
		this.RecordError(this.StatFH.Close())
	}
	if this.BiglongFH != nil {
		this.RecordError(this.BiglongFH.Close())
	}
}

func (this *ConfCmd) CloseResources() {
	if this.CloseReplication != nil {
		this.CloseReplication()
	}
	this.CloseFH()
	if this.FromDB != nil {
		this.FromDB.Close()
	}
}

func (this *ConfCmd) CloseChan() {
	if this.WorkType == "2sql" || this.WorkType == "rollback" {
		close(this.EventChan)
		close(this.StatChan)
	}

	if this.WorkType == "stats" {
		close(this.StatChan)
	}
}

func (this *ConfCmd) CreateDB() error {
	url := GetMysqlUrl(this)
	if this.WorkType == "binlogs" || this.ShouldAutoPosition() {
		url += "&timeout=5s&readTimeout=5s&writeTimeout=5s"
	}
	db, err := CreateMysqlCon(url)
	if err != nil {
		return fmt.Errorf("连接 MySQL 失败: %w", err)
	}
	this.FromDB = db
	return nil
}

func (this *ConfCmd) PrintUsageMsg() {
	fmt.Printf("%s\n", C_Version)
	flag.PrintDefaults()
}

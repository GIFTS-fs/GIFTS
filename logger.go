package gifts

import (
	"fmt"
	"io"
	"log"
	"os"
)

// global log switch
var giftsLogEnable bool = true

// globally (within one process) unique logger ID
var giftsLoggerID uint64 = 0

var giftsLogWritter io.Writer = os.Stderr

func setDefaultLogWritter(w io.Writer) {
	giftsLogWritter = w
}

// const logFilePrefix = "GIFTS_log_"

// struct_addr_pid_loggerID
const giftsLogFmt = "[%s_%s_%d_%d] "

// Logger is a wrapper for log.Logger.
// WARN: even if disabled, the string formating (perhaps) still takes CPU shares
type Logger struct {
	logger  *log.Logger
	Enabled bool // struct-level log control
}

// Printf of logger if log enabled
func (t *Logger) Printf(format string, v ...interface{}) {
	if giftsLogEnable && t.Enabled {
		t.logger.Printf(format, v...)
	}
}

// NewLogger is the constructor for gifts.Logger
func NewLogger(name, addr string, enabled bool) *Logger {
	prefix := fmt.Sprintf(giftsLogFmt, name, addr, os.Getpid(), giftsLoggerID)
	giftsLoggerID++

	return &Logger{logger: log.New(giftsLogWritter, prefix, log.LstdFlags), Enabled: enabled}
}

package core

import (
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// Logger manages proposal and commit logs, and persists them to file.
type Logger struct {
	conf         *Config
	proposingMap map[int]Log // key: proposal key, value: Log
	commitList   []Log       // committed logs
	commitSet    map[int]struct{}
	mu           sync.Mutex  // protects proposingMap and commitList
	logger       *log.Logger // file logger
	logFile      *os.File    // log file handle
}

func init() {
	// Register a zero value of the concrete type []core.Log
	gob.Register([]Log{})
}

// NewLogger initializes a Logger with file-based logging.
func NewLogger(conf *Config) *Logger {
	logDir := "../log/"
	if err := os.MkdirAll(logDir, 0777); err != nil {
		fmt.Println("failed to create log directory:", err)
	}
	logfile := logDir + "instancelog" + strconv.Itoa(int(conf.Id)) + ".log"
	f, err := os.OpenFile(logfile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777)
	if err != nil {
		fmt.Println("create log file failed:", err.Error())
		return nil
	}
	logger := log.New(f, "[Consensus log] ", log.LstdFlags|log.Lshortfile|log.LUTC|log.Lmicroseconds)
	return &Logger{
		conf:         conf,
		proposingMap: make(map[int]Log),
		commitList:   make([]Log, 0),
		commitSet:    make(map[int]struct{}),
		logger:       logger,
		logFile:      f,
	}
}

// Log writes a string message to the log file.
func (l *Logger) Log(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println(s)
}

// Propose logs a proposal message, adds it to the proposing map, and writes to file.
func (l *Logger) Propose(log Log) {
	estround := log.Estround
	key := Data2Key(log.Data)

	l.mu.Lock()
	l.proposingMap[estround] = log
	entryStr := fmt.Sprintf("generate Instance[%s] in seq %d at %s", key, estround, time.Now())
	l.logger.Println(entryStr)
	l.mu.Unlock()
}

// return proposing list
func (l *Logger) Merge(logs []Log) (proposing []int) {
	l.mu.Lock()
	for _, e := range logs {
		estround := e.Estround
		if _, ok := l.commitSet[estround]; ok {
			continue
		}
		l.proposingMap[estround] = e
		proposing = append(proposing, estround)
		key := Data2Key(e.Data)
		l.logger.Printf("generate Instance[%s] in seq %d at %s", key, estround, time.Now())
	}
	l.mu.Unlock()
	return
}

// Commit logs a commit message, adds it to the commit list, and writes to file.
func (l *Logger) Commit(estround int) {
	l.mu.Lock()
	logEntry := l.proposingMap[estround]
	delete(l.proposingMap, estround)
	l.commitList = append(l.commitList, logEntry)
	l.commitSet[logEntry.Estround] = struct{}{}
	key := Data2Key(logEntry.Data)
	l.logger.Printf("commit Instance[%s] in seq %d at %s\n", key, estround, time.Now())
	defer l.mu.Unlock()
}

// LoadHistory loads all log entries from the log file, sorted by estround.
func (l *Logger) LoadHistory() []Log {
	l.mu.Lock()
	defer l.mu.Unlock()
	dst := make([]Log, len(l.commitList))
	copy(dst, l.commitList)
	return dst
}

type Log struct {
	Estround int
	Data     []byte
}

// only Propose msg can compute key
func Data2Key(data []byte) string {
	hash := sha256.New()
	hash.Write(data)
	h := hash.Sum(nil)
	for i := range h {
		h[i] = 97 + h[i]%26
	}
	return string(h)
}

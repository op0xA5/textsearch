package main

import (
	"os"
	"time"
	"fmt"
	"runtime"
)

func FormatUnit(v float64) (float64, string) {
	if v < 0 {
		return 0, " "
	}
	if v < 1024 {
		return v, " "
	} else if v < 1024 * 1024 {
		return v / 1024, "K"
	} else if v < 1024 * 1024 * 1024 {
		return v / 1024 / 1024, "M"
	} else if v < 1024 * 1024 * 1024 * 1024 {
		return v / 1024 / 1024 / 1024, "G"
	}
	return v / 1024 / 1024 / 1024 / 1024, "T"
}

type StatInterface interface {
	ResetStat()
	PrintStat(d time.Duration, last bool)
}
func StatFunc(task string, i StatInterface, fn func()) {
	t := time.NewTicker(500 * time.Millisecond)
	end := make(chan int)

	timeStart := time.Now()
	lastTime  := timeStart
	i.ResetStat()
	go func() {
		for {
			select {
			case now := <- t.C:
				printf("\r> %8s  ", task)
				i.PrintStat(now.Sub(lastTime), false)
				lastTime = now
			case <- end:
				return
			}
		}
	}()

	fn()
	timeStop := time.Now()
	t.Stop()
	end <- 0
	printf("\r* %8s  ", task)
	i.PrintStat(timeStop.Sub(timeStart), true)
	printf("\n")
}

func printf(f string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, f, v...)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func lineStart(b []byte, start int) (i int) {
	if start >= len(b) {
		start = len(b) - 1
	}
	for i = start ; i > 0; i-- {
		if b[i] == '\n' {
			i++
			break
		}
	}
	return
}
func lineEnd(b []byte, start int) (i int) {
	for i = start; i < len(b); i++ {
		if b[i] == '\n' {
			if i > 0 && b[i-1] == '\r' {
				i--
			}
			break
		}
	}
	return
}

func handleErr(err error) bool {
	if err == nil {
		return false
	}
	callDepth := 1
	if _, ok := err.(errStr); ok {
		callDepth = 2
	}
	_, file, line, ok := runtime.Caller(callDepth)
	if !ok {
		file = "???"
		line = 0
	}
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			file = file[i+1:]
			break
		}
	}
	printf("error: %v  (%s:%d)\n", err, file, line)
	return true
}
type errStr struct {
	err string
}
func (err errStr) Error() string {
	return err.err
}
func handleErrStr(err string) {
	handleErr(errStr{ err })
}
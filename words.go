package main

import (
	"regexp"
	"time"
	"io"
	"os"
	"bufio"
	"sync/atomic"
)

type SplitFunc func(b []byte) []int
type WordSpliter struct {
	ByteTotal        int64
	lastByteCount    int64

	wordSpliteWorker
}

func NewWordSpliter(re string) (*WordSpliter, error) {
	r, err := regexp.Compile(re)
	if err != nil {
		return nil, err
	}
	ws := new(WordSpliter)
	ws.splitFn = func(b []byte) []int {
		m := r.FindSubmatchIndex(b)
		if len(m) > 2 {
			return m[2:]
		}
		return m
	}
	return ws, nil
}
func (ws *WordSpliter) ResetStat() {
	ws.lastByteCount = 0
}
func (ws *WordSpliter) PrintStat(d time.Duration, last bool) {
	var readTotal, readSpeed float64
	var readTotalUnit, readSpeedUnit string
	readTotal, readTotalUnit = FormatUnit(float64(ws.byteCount))
	if last {
		readSpeed, readSpeedUnit = FormatUnit(float64(ws.byteCount) / d.Seconds())
	} else {
		readSpeed, readSpeedUnit = FormatUnit(float64(ws.byteCount - ws.lastByteCount) / d.Seconds())
	}
	ws.lastByteCount = ws.byteCount
	if ws.ByteTotal > 0 {
		percentage := float64(100)
		if !last {
			percentage = (float64(ws.byteCount) / float64(ws.ByteTotal) * 100)
		}
		printf("Entry: %12d  Read: %6.1f%sB(%6.1f%sB/s)  Word(Min/Max): %6d/%6d [%5.1f%%]",
			ws.entryCount,
			readTotal, readTotalUnit, readSpeed, readSpeedUnit,
			ws.wordMin, ws.wordMax,
			percentage)
	} else {
		printf("Entry: %12d  Read: %6.1f%sB(%6.1f%sB/s)  Word(Min/Max): %6d/%6d",
			ws.entryCount,
			readTotal, readTotalUnit, readSpeed, readSpeedUnit,
			ws.wordMin, ws.wordMax)
	}
}
func (ws *WordSpliter) MeasureMulit(f *FileGroup, co int) (err error) {
	if co <= 1 {
		return ws.Measure(f)
	}

	var taskSeed int32
	finished := make(chan wordSpliterStats, co)
	workers := make([]*wordSpliteWorker, co)
	for i := 0; i < co; i++ {
		workers[i] = &wordSpliteWorker{
			splitFn: ws.splitFn,
		}
		go func(i int) {
			for {
				if err != nil {
					return
				}
				task := int(atomic.AddInt32(&taskSeed, 1) - 1)
				if task >= f.FileCount() {
					return
				}

				worker := workers[i]
				worker.offset = f.FileOffset(task)
				var file *os.File
				file, err = f.OpenFile(task)
				if err != nil {
					return
				}
				_, err = file.Seek(0, 0)
				if err != nil {
					return
				}
				err = worker.Measure(&io.LimitedReader{ file, f.FileSize(task) })
				if err != nil {
					return
				}
				finished <- worker.wordSpliterStats
			}
		}(i)
	}

	finishedStat := wordSpliterStats{}
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	var finishedCount int
	for {
		if err != nil {
			return
		}
		select {
		case v := <- finished:
			finishedStat = finishedStat.Merge(v)
			finishedCount++
			if finishedCount >= f.FileCount() {
				ws.wordSpliteWorker.wordSpliterStats = finishedStat
				return
			}

		case <- t.C:
			stat := finishedStat
			for _, worker := range workers {
				stat = stat.Merge(worker.wordSpliterStats)
			}
			ws.wordSpliteWorker.wordSpliterStats = stat
		}
	}
	return
}
func (ws *WordSpliter) ReadIntoIndexMulit(f *FileGroup, index *Index, co int) (err error) {
	if co <= 1 {
		return ws.ReadIntoIndex(f, index)
	}

	var taskSeed int32
	finished := make(chan wordSpliterStats, co)
	workers := make([]*wordSpliteWorker, co)
	for i := 0; i < co; i++ {
		workers[i] = &wordSpliteWorker{
			splitFn: ws.splitFn,
		}
		go func(i int) {
			for {
				if err != nil {
					return
				}
				task := int(atomic.AddInt32(&taskSeed, 1) - 1)
				if task >= f.FileCount() {
					return
				}
				worker := workers[i]
				worker.offset = f.FileOffset(task)
				var file *os.File
				file, err = f.OpenFile(task)
				if err != nil {
					return
				}
				_, err = file.Seek(0, 0)
				if err != nil {
					return
				}
				err = worker.ReadIntoIndex(&io.LimitedReader{ file, f.FileSize(task) }, index)
				if err != nil {
					return
				}
				finished <- worker.wordSpliterStats
			}
		}(i)
	}

	finishedStat := wordSpliterStats{}
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	var finishedCount int
	for {
		if err != nil {
			return
		}
		select {
		case v := <-finished:
			finishedStat = finishedStat.Merge(v)
			finishedCount++
			if finishedCount >= f.FileCount() {
				ws.wordSpliteWorker.wordSpliterStats = finishedStat
				return
			}

		case <- t.C:
			stat := finishedStat
			for _, worker := range workers {
				stat = stat.Merge(worker.wordSpliterStats)
			}
			ws.wordSpliteWorker.wordSpliterStats = stat
		}
	}
	return
}
func (ws *WordSpliter) WordStat() (min, max int) {
	return ws.wordMin, ws.wordMax
}
func (ws *WordSpliter) PosStat() (min, max int64) {
	return ws.posMin, ws.posMax
}
func (ws *WordSpliter) EntryCount() int {
	return ws.entryCount
}

type wordSpliteWorker struct {
	splitFn          SplitFunc
	offset           int64

	wordSpliterStats
}
func (ws *wordSpliteWorker) scanlines(r io.Reader, fn func(line []byte, offset int64)) (err error) {
	br := bufio.NewReader(r)
	ws.wordSpliterStats = wordSpliterStats{}
	offset := ws.offset
	var line []byte
	for {
		line, err = br.ReadBytes('\n')
		ws.byteCount += int64(len(line))
		if len(line) > 1 {
			end := len(line) - 2
			if line[end] == '\r' {
				end--
			}
			if end > 0 {
				fn(line[0:end], offset)
			}
		}
		offset += int64(len(line))

		if err == io.EOF {
			err = nil
			if fg, ok := r.(*FileGroup); ok && !fg.GroupEOF() {
				br.Reset(r)
				continue
			}
			return
		}
		if err != nil {
			return
		}
	}
}
func (ws *wordSpliteWorker) Measure(r io.Reader) error {
	return ws.scanlines(r, func(line []byte, offset int64) {
		m := ws.splitFn(line)
		for i := 1; i < len(m); i+=2 {
			start, end := m[i-1], m[i]
			if end > len(line) || start > len(line) || start < 0 || end < 0 {
				continue
			}
			count := end - start
			if count <= 0 {
				continue
			}
			ws.entryCount ++
			if ws.wordMin == 0 || ws.wordMin > count {
				ws.wordMin = count
			}
			if ws.wordMax < count {
				ws.wordMax = count
			}
			pos := offset + int64(start)
			if ws.posMin == 0 || ws.posMin > pos {
				ws.posMin = pos
			}
			if ws.posMax < pos {
				ws.posMax = pos
			}
		}
	})
}
func (ws *wordSpliteWorker) ReadIntoIndex(r io.Reader, index *Index) error {
	return ws.scanlines(r, func(line []byte, offset int64) {
		m := ws.splitFn(line)
		for i := 1; i < len(m); i+=2 {
			start, end := m[i-1], m[i]
			if end > len(line) || start > len(line) || start < 0 || end < 0 {
				continue
			}
			count := end - start
			if count <= 0 {
				continue
			}
			ws.entryCount++
			index.Push(offset + int64(start), count)
		}
	})
}

type wordSpliterStats struct {
	byteCount        int64
	entryCount       int
	wordMin, wordMax int
	posMin, posMax   int64
}
func (stat wordSpliterStats) Merge(n wordSpliterStats) wordSpliterStats {
	stat.byteCount += n.byteCount
	stat.entryCount += n.entryCount

	if stat.wordMin == 0 || stat.wordMin > n.wordMin {
		stat.wordMin = n.wordMin
	}
	if stat.wordMax < n.wordMax {
		stat.wordMax = n.wordMax
	}

	if stat.posMin == 0 || stat.posMin > n.posMin {
		stat.posMin = n.posMin
	}
	if stat.posMax < n.posMax {
		stat.posMax = n.posMax
	}
	return stat
}

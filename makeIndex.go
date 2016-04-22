package main

import (
	"os"
	"sort"
	"io"
	"time"
)

func makeIndex(base string, outfile string, pattern string) {
	ws, err := NewWordSpliter(pattern)
	if handleErr(err) { return }

	printf("Index Output: %s\n", outfile)
	indexFile, err := os.Create(outfile)
	if handleErr(err) { return }
	defer indexFile.Close()

	printf("Source: %s\n", base)
	f, err := NewFileGroupDirectory(base)
	if handleErr(err) { return }
	defer f.Close()
	ws.ByteTotal = f.Size()

	StatFunc("Measure", ws, func() {
		err = ws.MeasureMulit(f, coworkers)
		handleErr(err)
	})

	_, posMax := ws.PosStat()
	_, wordMax := ws.WordStat()
	posBits := uint(1)
	for ; 1 << posBits < posMax; posBits++ { }
	indexDatStruct := CalcIndexDataStruct(posMax, wordMax)
	totalSize, totalSizeUnit := FormatUnit(float64(ws.EntryCount() * int(posBits) / 8))
	memSize, memSizeUnit := FormatUnit(float64(indexDatStruct.Size(ws.EntryCount())))
	printf("            IndexSize: %6.1f%sB  MemSize: %6.1f%sB\n",
		totalSize, totalSizeUnit, memSize, memSizeUnit)

	printf("Prepare ...")
	_, err = f.Seek(0, 0)
	if handleErr(err) { return }
	index := NewIndex(ws.EntryCount(), indexDatStruct, f)

	StatFunc("Read", ws, func() {
		err = ws.ReadIntoIndexMulit(f, index, coworkers)
		handleErr(err)
	})

	StatFunc("Sorting", index, func() {
		sort.Sort(index)
	})
/*
	var isSort bool
	StatFunc("Checking", index, func() {
		isSort = sort.IsSorted(index)
	})
	if !isSort {
		handleErrStr("check failed")
		return
	}
*/
	printf("Write Index ...")
	indexFile.Write([]byte("INDEX"))
	indexFile.Write(f.DumpHead())

	indexW := new(indexWriter)
	StatFunc("WriteOut", indexW, func() {
		indexW.DoWrite(indexFile, index, posBits)
	})
}

type indexWriter struct {
	entryCount     int
	lastEntryCount int
	entryTotal     int
}
func (iw *indexWriter) DoWrite(file io.Writer, index *Index, posBits uint) {
	iw.entryTotal = index.Len()
	bw := NewBitWriter(file)
	bw.Write(uint64(posBits), 8)
	bw.Write(uint64(index.Len()), 56)
	for i := 0; i < index.Len(); i++ {
		bw.Write(uint64(index.GetPos(i)), posBits)
		iw.entryCount = i + 1
	}
	bw.Close()
}
func (iw *indexWriter) ResetStat() {
	iw.lastEntryCount = 0
}
func (iw *indexWriter) PrintStat(d time.Duration, last bool) {
	var entrySpeed float64
	var entrySpeedUnit string
	if last {
		entrySpeed, entrySpeedUnit = FormatUnit(float64(iw.entryCount) / d.Seconds())
	} else {
		entrySpeed, entrySpeedUnit = FormatUnit(float64(iw.entryCount - iw.lastEntryCount) / d.Seconds())
	}
	percentage := float64(100)
	if !last {
		percentage = (float64(iw.entryCount) / float64(iw.entryTotal) * 100)
	}
	printf("Entry: %12d(%6.1f%s/s) [%5.1f%%]",
		iw.entryCount,
		entrySpeed, entrySpeedUnit,
		percentage)

}

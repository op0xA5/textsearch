package main

import (
	"bytes"
	"time"
	"sync/atomic"
)

type IndexDataStruct struct {
	chunkLen    int
	posCount    int
	lengthCount int
}
func CalcIndexDataStruct(posMax int64, wordMax int) IndexDataStruct {
	ids := IndexDataStruct{}
	var u uint64 = 256
	for ids.posCount = 1; ids.posCount < 8; ids.posCount++ {
		if u > uint64(posMax) { break }
		u *= 256
	}
	u = 256
	for ids.lengthCount = 1 ; ids.lengthCount < 8; ids.lengthCount++ {
		if u > uint64(wordMax) { break }
		u *= 256
	}
	ids.chunkLen = ids.posCount + ids.lengthCount
	return ids
}
func (ids IndexDataStruct) Size(entryCount int) int {
	return entryCount * ids.chunkLen
}
func (ids IndexDataStruct) Get(dat []byte, i int) (pos int64, length int) {
	dat = dat[i * ids.chunkLen:]
	switch ids.posCount {
	case 1:
		pos = int64(dat[0])
	case 2:
		pos = int64(dat[0]) << 8 | int64(dat[1])
	case 3:
		pos = int64(dat[0]) << 16 | int64(dat[1]) << 8 | int64(dat[2])
	case 4:
		pos = int64(dat[0]) << 24 | int64(dat[1]) << 16 | int64(dat[2]) << 8 | int64(dat[3])
	case 5:
		pos = int64(dat[0]) << 32 | int64(dat[1]) << 24 | int64(dat[2]) << 16 | int64(dat[3]) << 8 |
			int64(dat[4])
	case 6:
		pos = int64(dat[0]) << 40 | int64(dat[1]) << 32 | int64(dat[2]) << 24 | int64(dat[3]) << 16 |
			int64(dat[4]) << 8 | int64(dat[5])

	case 7:
		pos = int64(dat[0]) << 48 | int64(dat[1]) << 40 | int64(dat[2]) << 32 | int64(dat[3]) << 24 |
			int64(dat[4]) << 16 | int64(dat[5]) << 8 | int64(dat[6])

	case 8:
		pos = int64(dat[0]) << 56 | int64(dat[1]) << 48 | int64(dat[2]) << 40 | int64(dat[3]) << 32|
			int64(dat[4]) << 24 | int64(dat[5]) << 16 | int64(dat[6]) << 8 | int64(dat[7])
	default:
		panic("should not into here")
	}
	dat = dat[ids.posCount:]
	switch ids.lengthCount {
	case 1:
		length = int(dat[0])
	case 2:
		length = int(dat[0]) << 8 | int(dat[1])
	case 3:
		length = int(dat[0]) << 16 | int(dat[1]) << 8 | int(dat[2])
	case 4:
		length = int(dat[0]) << 24 | int(dat[1]) << 16 | int(dat[2]) << 8 | int(dat[3])
	default:
		panic("should not into here")
	}
	return
}
func (ids IndexDataStruct) Put(dat []byte, i int, pos int64, length int) {
	dat = dat[i * ids.chunkLen:]
	switch ids.posCount {
	case 1:
		dat[0] = byte(pos)
	case 2:
		dat[0] = byte(pos >> 8)
		dat[1] = byte(pos)
	case 3:
		dat[0] = byte(pos >> 16)
		dat[1] = byte(pos >> 8)
		dat[2] = byte(pos)
	case 4:
		dat[0] = byte(pos >> 24)
		dat[1] = byte(pos >> 16)
		dat[2] = byte(pos >> 8)
		dat[3] = byte(pos)
	case 5:
		dat[0] = byte(pos >> 32)
		dat[1] = byte(pos >> 24)
		dat[2] = byte(pos >> 16)
		dat[3] = byte(pos >> 8)
		dat[4] = byte(pos)
	case 6:
		dat[0] = byte(pos >> 40)
		dat[1] = byte(pos >> 32)
		dat[2] = byte(pos >> 24)
		dat[3] = byte(pos >> 16)
		dat[4] = byte(pos >> 8)
		dat[5] = byte(pos)
	case 7:
		dat[0] = byte(pos >> 48)
		dat[1] = byte(pos >> 40)
		dat[2] = byte(pos >> 32)
		dat[3] = byte(pos >> 24)
		dat[4] = byte(pos >> 16)
		dat[5] = byte(pos >> 8)
		dat[6] = byte(pos)
	case 8:
		dat[0] = byte(pos >> 56)
		dat[1] = byte(pos >> 48)
		dat[2] = byte(pos >> 40)
		dat[3] = byte(pos >> 32)
		dat[4] = byte(pos >> 24)
		dat[5] = byte(pos >> 16)
		dat[6] = byte(pos >> 8)
		dat[7] = byte(pos)
	default:
		panic("should not into here")
	}
	dat = dat[ids.posCount:]
	switch ids.lengthCount {
	case 1:
		dat[0] = byte(length)
	case 2:
		dat[0] = byte(length >> 8)
		dat[1] = byte(length)
	case 3:
		dat[0] = byte(length >> 16)
		dat[1] = byte(length >> 8)
		dat[2] = byte(length)
	case 4:
		dat[0] = byte(length >> 24)
		dat[1] = byte(length >> 16)
		dat[2] = byte(length >> 8)
		dat[3] = byte(length)
	default:
		panic("should not into here")
	}
	return
}
func (ids IndexDataStruct) Swap(dat []byte, i, j int) {
	if i == j { return }
	a := dat[i * ids.chunkLen:]
	b := dat[j * ids.chunkLen:]
	switch ids.chunkLen {
	case 1:
		a[0], b[0] = b[0], a[0]
	case 2:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
	case 3:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
	case 4:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
	case 5:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
	case 6:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
	case 7:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
		a[6], b[6] = b[6], a[6]
	case 8:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
		a[6], b[6] = b[6], a[6]
		a[7], b[7] = b[7], a[7]
	case 9:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
		a[6], b[6] = b[6], a[6]
		a[7], b[7] = b[7], a[7]
		a[8], b[8] = b[8], a[8]
	case 10:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
		a[6], b[6] = b[6], a[6]
		a[7], b[7] = b[7], a[7]
		a[8], b[8] = b[8], a[8]
		a[9], b[9] = b[9], a[9]
	case 11:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
		a[6], b[6] = b[6], a[6]
		a[7], b[7] = b[7], a[7]
		a[8], b[8] = b[8], a[8]
		a[9], b[9] = b[9], a[9]
		a[10], b[10] = b[10], a[10]
	case 12:
		a[0], b[0] = b[0], a[0]
		a[1], b[1] = b[1], a[1]
		a[2], b[2] = b[2], a[2]
		a[3], b[3] = b[3], a[3]
		a[4], b[4] = b[4], a[4]
		a[5], b[5] = b[5], a[5]
		a[6], b[6] = b[6], a[6]
		a[7], b[7] = b[7], a[7]
		a[8], b[8] = b[8], a[8]
		a[9], b[9] = b[9], a[9]
		a[10], b[10] = b[10], a[10]
		a[11], b[11] = b[11], a[11]
	default:
		panic("should not into here")
	}
}

type Index struct {
	dat       []byte
	datStruct IndexDataStruct
	datCount  int64
	pool      *FileGroup

	swapCount        int64
	compareCount     int64
	lastSwapCount    int64
	lastCompareCount int64

	regA, regB int
	regAV, regBV []byte
}
func NewIndex(entryCount int, datStruct IndexDataStruct, pool *FileGroup) *Index {
	datSize := datStruct.Size(entryCount)
	return &Index{
		dat: make([]byte, datSize),
		datStruct: datStruct,
		pool: pool,

		regA: -1,
		regB: -1,
	}
}
func (idx *Index) Push(pos int64, length int) {
	i := int(atomic.AddInt64(&idx.datCount, 1)) - 1
	idx.datStruct.Put(idx.dat, i, pos, length)
}
func (idx *Index) Len() int           { return int(idx.datCount) }
func (idx *Index) Swap(i, j int)      {
	idx.swapCount++

	if idx.regA == i {
		idx.regA = j
	} else if idx.regA == j {
		idx.regA = i
	}
	if idx.regB == i {
		idx.regB = j
	} else if idx.regB == j {
		idx.regB = i
	}

	idx.datStruct.Swap(idx.dat, i, j)
}
func (idx *Index) Less(i, j int) bool {
	idx.compareCount++

	var a, b []byte
	switch {
	case i == idx.regA:
		a = idx.regAV
		b = idx.Get(j)
		idx.regB = j
		idx.regBV = b
	case i == idx.regB:
		a = idx.regBV
		b = idx.Get(j)
		idx.regA = j
		idx.regAV = b
	case j == idx.regB:
		b = idx.regBV
		a = idx.Get(i)
		idx.regA = i
		idx.regAV = a
	case j == idx.regA:
		b = idx.regAV
		a = idx.Get(i)
		idx.regB = i
		idx.regBV = a
	default:
		a = idx.Get(i)
		idx.regA = i
		idx.regAV = a
		b = idx.Get(j)
		idx.regB = j
		idx.regBV = b
	}
	return bytes.Compare(a, b) == -1
}
func (idx *Index) Get(i int) []byte {
	pos, length := idx.datStruct.Get(idx.dat, i)
	dat, err := idx.pool.ReadMapper(pos, length)
	handleErr(err)
	return dat
}
func (idx *Index) GetPos(i int) int64 {
	pos, _ := idx.datStruct.Get(idx.dat, i)
	return pos
}
func (idx *Index) ResetStat() {
	idx.swapCount = 0
	idx.compareCount = 0
	idx.lastSwapCount = 0
	idx.lastCompareCount = 0
}
func (idx *Index) PrintStat(d time.Duration, last bool) {
	var compSpeed, swapSpeed float64
	var compSpeedUnit, swapSpeedUnit string
	if last {
		compSpeed, compSpeedUnit = FormatUnit(float64(idx.compareCount) / d.Seconds())
		swapSpeed, swapSpeedUnit = FormatUnit(float64(idx.swapCount) / d.Seconds())
	} else {
		compSpeed, compSpeedUnit = FormatUnit(float64(idx.compareCount - idx.lastCompareCount) / d.Seconds())
		swapSpeed, swapSpeedUnit = FormatUnit(float64(idx.swapCount - idx.lastSwapCount) / d.Seconds())
	}
	printf(" Comp: %12d(%10.3f) %6.1f%s/s  Swap: %12d(%10.3fs) %6.1f%s/s",
		idx.compareCount, float64(idx.compareCount) / float64(idx.datCount), compSpeed, compSpeedUnit,
		idx.swapCount, float64(idx.swapCount) / float64(idx.datCount), swapSpeed, swapSpeedUnit)
	idx.lastCompareCount = idx.compareCount
	idx.lastSwapCount = idx.swapCount
}


package main

import (
	"fmt"
	"os"
	"io"
	"bytes"
)

func searchIndex(base, index string, q []byte) {
	fidx, err := os.Open(index)
	if handleErr(err) { return }
	defer fidx.Close()

	buf5 := make([]byte, 5)
	_, err = io.ReadFull(fidx, buf5)
	if handleErr(err) { return }
	if string(buf5) != "INDEX" {
		handleErrStr("not index file")
		return
	}
	f, err := NewFileGroupReadHead(base, fidx)
	if handleErr(err) { return }
	defer f.Close()

	br := NewBitReader(fidx)
	br.Base, err = fidx.Seek(0, 1)

	var v uint64
	v, err = br.ReadAt(0, 8)
	if handleErr(err) { return }
	posBits := int64(v)
	v, err = br.ReadAt(8, 56)
	if handleErr(err) { return }
	indexNum := int64(v)
	br.Base += 8

	buf := make([]byte, 4 * 1024)
	if len(q) > len(buf) {
		q = q[0:len(buf)]
	}
	ns, err := bsearch(indexNum, func(i int64) (bool, error) {
		offset, err := br.ReadAt(i * posBits, posBits)
		if handleErr(err) { return false, err }

		str, err := f.ReadAt(int64(offset), buf[0:len(q)])
		if handleErr(err) { return false, err }
		return bytes.Compare(str[0:lineEnd(str, 0)], q) >= 0, nil
	})
	if err != nil { return }
	for ; ns < indexNum; ns++ {
		offset, err := br.ReadAt(ns * posBits, posBits)
		if handleErr(err) { return }

		base := (int64(offset) / 1024 - 1) * 1024
		if (base < 0) { base = 0 }

		str, err := f.ReadAt(base, buf)
		if handleErr(err) { return }
		offsetBuf := int(int64(offset) - base)
		qEndBuf := offsetBuf + min(len(q), len(str))
		if bytes.Compare(str[offsetBuf : qEndBuf], q) == 0 {
			filename, _ := f.Filename(int64(offset))
			lineLeft := str[lineStart(str, offsetBuf) : offsetBuf]
			lineRight := str[qEndBuf : lineEnd(str, qEndBuf)]

			fmt.Fprintf(os.Stdout, "%s: %s\033[32m%s\033[0m%s\n",
				filename, lineLeft, q, lineRight)
		} else {
			break
		}
	}
}

func bsearch(n int64, f func(int64) (bool, error)) (int64, error) {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := int64(0), n
	for i < j {
		h := i + (j-i)/2 // avoid overflow when computing h
		// i ≤ h < j
		result, err := f(h)
		if err != nil {
			return i, err
		}
		if !result {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i, nil
}



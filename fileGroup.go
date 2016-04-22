package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path"
	"io"
	"syscall"
)

type FileGroup struct {
	base    string
	names   []string
	sizes   []int64
	offsets []int64
	handles []*os.File
	mapper  [][]byte

	current       int
	currentHandle *os.File
	currentRemain int64

	totalSize int64
}
func NewFileGroupDirectory(base string) (fg *FileGroup, err error) {
	var d *os.File
	d, err = os.Open(base)
	if err != nil {
		return
	}
	var fi []os.FileInfo
	fi, err = d.Readdir(-1)
	if err != nil {
		return
	}
	fg = &FileGroup{
		base:  base,
		names: make([]string, 0, len(fi)),
		sizes: make([]int64, 0, len(fi)),
		offsets: make([]int64, 0, len(fi)),
	}
	var sum int64
	for _, file := range fi {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if filename[0] == '.' || len(filename) > 0xFFFF {
			continue
		}
		size := file.Size()
		if size == 0 {
			continue
		}
		fg.names = append(fg.names, filename)
		fg.sizes = append(fg.sizes, size)
		fg.offsets = append(fg.offsets, sum)
		sum += size
	}
	fg.totalSize = sum
	fg.Reset()
	return
}
func NewFileGroupReadHead(base string, h *os.File) (fg *FileGroup, err error) {
	buf8 := make([]byte, 8)
	names := make([]string, 0, 16)
	sizes := make([]int64, 0, 16)
	offsets := make([]int64, 0, 16)
	var sum int64
	for {
		_, err = io.ReadFull(h, buf8)
		if err != nil {
			return
		}
		v := binary.BigEndian.Uint64(buf8)
		size := int64(v & 0x0000FFFFFFFFFFFF)
		nameLen := int(v >> 48)
		nameBuf := make([]byte, nameLen)
		_, err = io.ReadFull(h, nameBuf)
		if err != nil {
			return
		}
		if size == 0 {
			break
		}
		names = append(names, string(nameBuf))
		sizes = append(sizes, size)
		offsets = append(offsets, sum)
		sum += size
	}
	fg = &FileGroup{
		base:  base,
		names: names,
		sizes: sizes,
		offsets: offsets,
		totalSize: sum,
	}
	fg.Reset()
	return
}

func (fg *FileGroup) DumpHead() []byte {
	buf := new(bytes.Buffer)

	buf8 := make([]byte, 8)
	for i, size := range fg.sizes {
		n := uint64(size) & 0x0000FFFFFFFFFFFF
		name := fg.names[i]
		n |= uint64(len(name)) << 48
		binary.BigEndian.PutUint64(buf8, n)
		buf.Write(buf8)
		buf.WriteString(name)
	}
	name := "https://github.com/yurinacn/textindex"
	n := uint64(0)
	n |= uint64(len(name)) << 48
	binary.BigEndian.PutUint64(buf8, n)
	buf.Write(buf8)
	buf.WriteString(name)
	return buf.Bytes()
}
func (fg *FileGroup) Reset() {
	if fg.handles == nil {
		fg.handles = make([]*os.File, len(fg.sizes))
	}
	fg.current = 0
	fg.currentHandle = nil
	if fg.current < len(fg.sizes) {
		fg.currentRemain = fg.sizes[0]
	}
}
func (fg *FileGroup) Read(p []byte) (n int, err error) {
	if fg.current >= len(fg.sizes) {
		return 0, io.EOF
	}
	if fg.currentHandle == nil {
		fg.currentHandle, err = fg.OpenFile(fg.current)
		if err != nil {
			return 0, err
		}
		_, err = fg.currentHandle.Seek(fg.sizes[fg.current] - fg.currentRemain, 0)
		if err != nil {
			return 0, err
		}
	}

	if int64(len(p)) > fg.currentRemain {
		p = p[0:fg.currentRemain]
	}
	n, err = fg.currentHandle.Read(p)
	fg.currentRemain -= int64(n)
	if err == io.EOF {
		fg.currentRemain = 0
	}

	if fg.currentRemain <= 0 {
		fg.current++
		fg.currentHandle = nil
		if fg.current < len(fg.sizes) {
			fg.currentRemain = fg.sizes[fg.current]
		}
		err = io.EOF
	}

	return
}
func (fg *FileGroup) ReadAt(offset int64, buf []byte) ([]byte, error)  {
	i := fg.OffsetIndex(offset)
	offset = offset - fg.offsets[i]
	h, err := fg.OpenFile(i)
	if err != nil {
		return nil, err
	}
	_, err = h.Seek(offset, 0)
	if err != nil { return nil, err }
	buf = buf[0:min(len(buf), int(fg.sizes[i] - offset))]
	_, err = io.ReadFull(h, buf)
	if err != nil { return nil, err }

	if i == fg.current {
		fg.currentHandle = nil
	}
	return buf, nil
}
var errReadMapperOverFile = errors.New("read mapper over single file")
func (fg *FileGroup) ReadMapper(offset int64, length int) ([]byte, error) {
	i := fg.OffsetIndex(offset)
	offset = offset - fg.offsets[i]
	size := fg.sizes[i]
	if offset + int64(length) > size {
printf("Read File: #%d  %s\n", i, fg.names[i])
printf("offset %d length %d availSize %d\n", offset, length, size)
panic(errReadMapperOverFile)
		return nil, errReadMapperOverFile
	}

	if fg.mapper == nil {
		fg.mapper = make([][]byte, len(fg.sizes))
	}
	b := fg.mapper[i]
	if b == nil {
		h, err := fg.OpenFile(i)
		if err != nil {
			return nil, err
		}
		b, err = syscall.Mmap(int(h.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}
		fg.mapper[i] = b
	}
	return b[offset : offset + int64(length)], nil
}
var ErrNotSupported = errors.New("not supported")
var ErrOutOfRange   = errors.New("out of range")
func (fg *FileGroup) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		if offset > fg.totalSize {
			return 0, ErrOutOfRange
		}
		i := fg.OffsetIndex(offset)
		fg.current = i
		fg.currentHandle = nil
		fg.currentRemain = fg.offsets[i] + fg.sizes[i] - offset
		return offset, nil
	}
	return 0, ErrNotSupported
}
func (fg *FileGroup) Close() (err error) {
	for i, f := range fg.handles {
		if f != nil {
			e := f.Close()
			if e != nil { err = e }
			fg.handles[i] = nil
		}
	}
	for i, m := range fg.mapper {
		if m != nil {
			syscall.Munmap(m)
			fg.mapper[i] = nil
		}
	}
	return
}
func (fg *FileGroup) Filename(offset int64) (string, error) {
	i := fg.OffsetIndex(offset)
	if i >= len(fg.names) {
		return "", ErrOutOfRange
	}
	return fg.names[i], nil
}
func (fg *FileGroup) CurrentFilename() string {
	if fg.current < len(fg.names) {
		return fg.names[fg.current]
	}
	return ""
}
func (fg *FileGroup) GroupEOF() bool {
	return fg.current >= len(fg.sizes)
}
func (fg *FileGroup) Size() int64 {
	return fg.totalSize
}
func (fg *FileGroup) FileSize(i int) int64 {
	return fg.sizes[i]
}
func (fg *FileGroup) FileCount() int {
	return len(fg.sizes)
}
func (fg *FileGroup) FileOffset(i int) int64 {
	return fg.offsets[i]
}
func (fg *FileGroup) OpenFile(i int) (h *os.File, err error) {
	h = fg.handles[i]
	if h == nil {
		h, err = os.Open(path.Join(fg.base, fg.names[i]))
		if err != nil {
			return
		}
		fg.handles[i] = h
	}
	return
}
func (fg *FileGroup) OffsetIndex(offset int64) int {
	i, j := 0, len(fg.offsets)
	for i < j {
		h := i + (j-i)/2
		if offset >= fg.offsets[h] {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

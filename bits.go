package main

import (
	"io"
)

type BitReader struct {
	Base int64
	r    io.ReadSeeker
}
func NewBitReader(r io.ReadSeeker) *BitReader {
	return &BitReader{
		r: r,
	}
}

func (br *BitReader) ReadAt(pos, bit int64) (p uint64, err error) {
	bytePos := br.Base + pos / 8
	pos = pos % 8
	byteLen := (pos + bit) / 8
	if (pos + bit) % 8 > 0 {
		byteLen++
	}
	_, err = br.r.Seek(bytePos, 0)
	if err != nil {
		return
	}
	
	buf := make([]byte, byteLen)
	_, err = io.ReadFull(br.r, buf)
	if err != nil {
		return
	}
	if len(buf) > 0 { p |= uint64(buf[0]) << 56 }
	if len(buf) > 1 { p |= uint64(buf[1]) << 48 }
	if len(buf) > 2 { p |= uint64(buf[2]) << 40 }
	if len(buf) > 3 { p |= uint64(buf[3]) << 32 }
	if len(buf) > 4 { p |= uint64(buf[4]) << 24 }
	if len(buf) > 5 { p |= uint64(buf[5]) << 16 }
	if len(buf) > 6 { p |= uint64(buf[6]) << 8 }
	if len(buf) > 7 { p |= uint64(buf[7]) }

	for i := uint(0); i < uint(pos); i++ {
		p &= ^(1 << (63 - i))
	}
	p = p >> uint(64 - pos - bit)
	return
}

type BitWriter struct {
	w   io.Writer
	buf []byte
	bit int64
}
func NewBitWriter(w io.Writer) *BitWriter {
	return &BitWriter{
		w: w,
		buf: make([]byte, 64 * 1024),
	}
}
func (bw *BitWriter) Write(p uint64, bit uint) error {
	if bit > 64 - 8 {
		panic("bit too long")
	}
	if bw.bit + int64(bit) + 72 > int64(len(bw.buf)) * 8 {
		err := bw.Flush()
		if err != nil {
			return err
		}
	}

	p = p << (64 - uint(bw.bit % 8) - bit)
	n := bw.bit / 8
	bw.buf[n] |= byte(p >> 56)
	bw.buf[n+1] = byte(p >> 48)
	bw.buf[n+2] = byte(p >> 40)
	bw.buf[n+3] = byte(p >> 32)
	bw.buf[n+4] = byte(p >> 24)
	bw.buf[n+5] = byte(p >> 16)
	bw.buf[n+6] = byte(p >> 8)
	bw.buf[n+7] = byte(p)
	bw.buf[n+8] = 0
	bw.bit += int64(bit)
	return nil
}
func (bw *BitWriter) Flush() error {
	byteLen := int(bw.bit / 8)
	if byteLen == 0 {
		return nil
	}

	n, err := bw.w.Write(bw.buf[0:byteLen])
	if err != nil {
		return err
	}
	if n != byteLen {
		return io.ErrShortWrite
	}
	for i := 0; i < byteLen; i++ {
		bw.buf[i] = 0
	}
	if byteLen < len(bw.buf) {
		bw.buf[0] = bw.buf[byteLen]
		bw.buf[byteLen] = 0
	}
	bw.bit = bw.bit & 0x07
	return nil
}
func (bw *BitWriter) Close() error {
	if bw.bit > 0 {
		byteLen := int(bw.bit / 8)
		if bw.bit % 8 > 0 {
			byteLen++
		}
		n, err := bw.w.Write(bw.buf[0:byteLen])
		if err != nil {
			return err
		}
		if n != byteLen {
			return io.ErrShortWrite
		}
		bw.bit = 0
	}
	return nil
}

func GenerateMask(bits uint) (m uint64) {
	for i := uint(0); i < bits; i++ {
		m |= 1 << i
	}
	return
}

package sql

import "bytes"

const TabSpace = 4

type SQLBuffer struct {
	buf *bytes.Buffer
}

func NewSQLBuffer() *SQLBuffer {
	return &SQLBuffer{bytes.NewBuffer(nil)}
}

func NewSQLBufferByte(buf []byte) *SQLBuffer {
	return &SQLBuffer{bytes.NewBuffer(buf)}
}

func NewSQLBufferString(v string) *SQLBuffer {
	return &SQLBuffer{bytes.NewBufferString(v)}
}

func (s *SQLBuffer) WriteString(v string) {
	s.buf.WriteString(v)
}

func (s *SQLBuffer) WriteStringln(v string) {
	s.WriteString(v)
	_ = s.WriteByte('\n')
}

func (s *SQLBuffer) WriteNTabStringln(v string, tabCount int) {
	for i := 0; i < tabCount; i++ {
		s.WriteByteN(' ', TabSpace)
	}
	s.WriteString(v)
	_ = s.WriteByte('\n')
}

func (s *SQLBuffer) WriteByte(b byte) error {
	return s.buf.WriteByte(b)
}

func (s *SQLBuffer) WriteByteN(b byte, n int) {
	for i := 0; i < n; i++ {
		_ = s.WriteByte(b)
	}
}

func (s *SQLBuffer) Write(b []byte) (int, error) {
	return s.buf.Write(b)
}

func (s *SQLBuffer) Reset() {
	s.buf.Reset()
}

func (s *SQLBuffer) Truncate(n int) {
	s.buf.Truncate(n)
}

func (s *SQLBuffer) Len() int {
	return s.buf.Len()
}

func (s *SQLBuffer) Bytes() []byte {
	return s.buf.Bytes()
}

func (s *SQLBuffer) String() string {
	return s.buf.String()
}

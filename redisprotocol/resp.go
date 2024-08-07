package redisprotocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	Type   string
	Str    string
	Num    int
	Bulk   string
	Array  []Value
}

type Resp struct {
	reader *bufio.Reader
	writer io.Writer
}

func NewResp(rd io.Reader, wr io.Writer) *Resp {
	return &Resp{
		reader: bufio.NewReader(rd),
		writer: wr,
	}
}

// Reader methods
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

func (r *Resp) readArray() (Value, error) {
	v := Value{Type: "array"}
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.Array = make([]Value, 0, len)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.Array = append(v.Array, val)
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{Type: "bulk"}
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return v, err
	}
	v.Bulk = string(bulk)

	// Read the trailing CRLF
	r.readLine()

	return v, nil
}

// Writer methods
func (r *Resp) Write(v Value) error {
	switch v.Type {
	case "array":
		return r.writeArray(v.Array)
	case "bulk":
		return r.writeBulk(v.Bulk)
	case "string":
		return r.writeString(v.Str)
	case "error":
		return r.writeError(v.Str)
	case "integer":
		return r.writeInteger(v.Num)
	default:
		return fmt.Errorf("unknown type: %v", v.Type)
	}
}

func (r *Resp) writeArray(arr []Value) error {
	_, err := fmt.Fprintf(r.writer, "*%d\r\n", len(arr))
	if err != nil {
		return err
	}
	for _, v := range arr {
		err := r.Write(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Resp) writeBulk(s string) error {
	_, err := fmt.Fprintf(r.writer, "$%d\r\n%s\r\n", len(s), s)
	return err
}

func (r *Resp) writeString(s string) error {
	_, err := fmt.Fprintf(r.writer, "+%s\r\n", s)
	return err
}

func (r *Resp) writeError(s string) error {
	_, err := fmt.Fprintf(r.writer, "-%s\r\n", s)
	return err
}

func (r *Resp) writeInteger(i int) error {
	_, err := fmt.Fprintf(r.writer, ":%d\r\n", i)
	return err
}
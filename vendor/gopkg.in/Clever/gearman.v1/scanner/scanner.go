package scanner

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

func needMoreData() (int, []byte, error) { return 0, nil, nil }

const headerSize = 12

// New returns a new Scanner that parses a Reader as the Gearman protocol.
// See: http://gearman.org/protocol/
func New(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if len(data) < headerSize {
			return needMoreData()
		}

		var size int32
		if err := binary.Read(bytes.NewBuffer(data[8:12]), binary.BigEndian, &size); err != nil {
			return 0, nil, err
		}

		if len(data) < headerSize+int(size) {
			return needMoreData()
		}

		// bufio.Scanner reuses these bytes, so make sure we copy them.
		var packet = make([]byte, int(headerSize+size))
		copy(packet, data[0:headerSize+size])
		return int(headerSize + size), packet, nil
	})
	return scanner
}

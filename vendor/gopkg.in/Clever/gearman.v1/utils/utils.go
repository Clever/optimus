package utils

import (
	"bytes"
)

// Buffer is a WriteCloser that wraps a bytes.Buffer
type Buffer struct {
	bytes.Buffer
}

// Close noop
func (b Buffer) Close() error {
	return nil
}

// NewBuffer initializes an empty Buffer
func NewBuffer() *Buffer {
	return &Buffer{}
}

package csv

import (
	"encoding/csv"
	"fmt"
	"github.com/azylman/getl"
	"os"
)

func convertRowToRecord(row getl.Row, headers []string) []string {
	record := []string{}
	for _, header := range headers {
		record = append(record, fmt.Sprintf("%v", row[header]))
	}
	return record
}

func convertRowToHeader(row getl.Row) []string {
	header := []string{}
	for key := range row {
		header = append(header, key)
	}
	return header
}

// New writes all of the Rows in a Table to a CSV file.
func New(source getl.Table, filename string) error {
	fout, err := os.Create(filename)
	defer fout.Close()
	if err != nil {
		return err
	}
	writer := csv.NewWriter(fout)
	headers := []string{}
	wroteHeader := false
	for row := range source.Rows() {
		if !wroteHeader {
			headers = convertRowToHeader(row)
			if err := writer.Write(headers); err != nil {
				return err
			}
			wroteHeader = true
		}
		if err := writer.Write(convertRowToRecord(row, headers)); err != nil {
			return err
		}
	}
	if source.Err() != nil {
		return source.Err()
	}
	writer.Flush()
	if writer.Error() != nil {
		return writer.Error()
	}
	return nil
}

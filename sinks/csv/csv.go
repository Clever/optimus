package csv

import (
	"encoding/csv"
	"fmt"
	"gopkg.in/azylman/optimus.v1"
	"os"
)

func convertRowToRecord(row optimus.Row, headers []string) []string {
	record := []string{}
	for _, header := range headers {
		if row[header] == nil {
			row[header] = ""
		}
		record = append(record, fmt.Sprintf("%v", row[header]))
	}
	return record
}

func convertRowToHeader(row optimus.Row) []string {
	header := []string{}
	for key := range row {
		header = append(header, key)
	}
	return header
}

// New writes all of the Rows in a Table to a CSV file.
func New(source optimus.Table, filename string) error {
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

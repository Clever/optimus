package csv

import (
	"encoding/csv"
	"fmt"
	"gopkg.in/azylman/optimus.v1"
	"io"
	"sort"
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

// New writes all of the Rows in a Table to a CSV file. It assumes that all Rows have the same
// headers. Columns are written in alphabetical order.
func New(out io.Writer) optimus.Sink {
	return func(source optimus.Table) error {
		writer := csv.NewWriter(out)
		headers := []string{}
		wroteHeader := false
		for row := range source.Rows() {
			if !wroteHeader {
				headers = convertRowToHeader(row)
				sort.Strings(headers)
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
}

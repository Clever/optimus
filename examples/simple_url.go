package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/Clever/optimus/transformer"
	"gopkg.in/Clever/optimus.v3"
	jsonSink "gopkg.in/Clever/optimus.v3/sinks/json"
	csvSource "gopkg.in/Clever/optimus.v3/sources/csv"
)

/*
In this example, we will:
1) read in data from CSV
2) filter and transform that data in a simple way
3) write the data to JSON
4) output the number of rows we read in and the number of rows after filtering

It's possible make an even simpler example, but this is a little more interesting.
*/

var (
	inputFilename  = filepath.Join("examples", "urls.csv")
	outputFilename = filepath.Join("examples", "urls.json")
	cleverRegex, _ = regexp.Compile("clever.com.*")
)

// helper function to keep things clean
func fatalIfErr(err error) {
	if err != nil {
		log.Fatalf("error in URL example! Message is: %s", err)
	}
}

// filterItems will only output SSL=true URL=/clever.com.*/ entries
// if SSL or URL are missing, don't bother erroring and instead drop the row
func filterItems(row optimus.Row) (bool, error) {
	url, ok := row["url"]
	if !ok || !cleverRegex.MatchString(url.(string)) {
		return false, nil
	}
	ssl, err := strconv.ParseBool(row["ssl"].(string))
	if err != nil || ssl == false {
		return false, nil
	}

	return true, nil
}

// addProtocol prepends the URL with "https://" if SSL=true, else "http://" if false
// also removes the SSL item from the row
func addProtocol(row optimus.Row) (optimus.Row, error) {
	url, ok := row["url"]
	if !ok {
		return nil, fmt.Errorf("Error getting URL from row, row: %s", row)
	}
	ssl, err := strconv.ParseBool(row["ssl"].(string))
	if err != nil {
		return nil, fmt.Errorf("Issue parsing ssl for row with URL: %s", url)
	}

	protocol := "http"
	if ssl {
		protocol = "https"
	}

	// note that rows are maps, and are therefore mutable
	// strictly speaking, you would not have to return the row
	// and this can bite you if you're not careful with Each or other
	// transforms that don't suggest the table is modified
	row["url"] = fmt.Sprintf("%s://%s", protocol, url)
	delete(row, "ssl")
	return row, nil
}

func main() {
	log.Printf("Filtering on 'ssl'=true and this regex: '%s'", cleverRegex)
	// create open csv "source" file and json "sink" file
	inputFile, err := os.Open(inputFilename)
	fatalIfErr(err)
	outputFile, err := os.Create(outputFilename)
	fatalIfErr(err)

	// create the "source" we pull from
	// note that this automatically handles the conversion from a CSV row to an optimus "row"
	cSource := csvSource.New(inputFile)
	// create the "sink" we push to
	// note that this automatically handles marshalling to JSON
	jSink := jsonSink.New(outputFile)

	// let's set up some counting variables for fun
	beforeCounter := 0
	afterCounter := 0

	// set up and start the transform
	err = transformer.New(cSource).
		// start by counting how many we read
		Each(func(r optimus.Row) error {
		log.Println("BEFORE: ", r)
		beforeCounter++
		return nil
	}).
		// as a simple case, let's only match "clever.com" hosts with SSL true
		Select(filterItems).
		// then let's just append "https" onto those urls
		Map(addProtocol).
		// finish by counting how many we end up writing
		Each(func(r optimus.Row) error {
		log.Println("AFTER: ", r)
		afterCounter++
		return nil
	}).
		// set up the sink
		Sink(jSink)

	fatalIfErr(err)
	log.Printf("Done processing, ingested %d items, wrote %d items out", beforeCounter, afterCounter)
}

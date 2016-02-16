package transformer

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"gopkg.in/Clever/optimus.v3"
	jsonSink "gopkg.in/Clever/optimus.v3/sinks/json"
	csvSource "gopkg.in/Clever/optimus.v3/sources/csv"
)

/*
Example_transformCSVdata will
1) read in data from CSV
2) filter and transform that data in a simple way
3) write the data to JSON
4) output the number of rows we read in and the number of rows after filtering

It's possible make an even simpler example, but this is a little more interesting.
*/
func Example_transformCSVData() {
	var data = `url,ssl,count
https://google.com/baz,true,8
https://clever.com/foo,true,5
http://facebook.com,false,10
https://api.clever.com/bar,true,2
http://www.clever.info/bar,false,3
`
	// create the "source" we pull from
	// note that this automatically handles the conversion from a CSV row to an optimus "row"
	cSource := csvSource.New(bytes.NewBufferString(data)) // gopkg.in/Clever/optimus.v3/sources/csv

	// create the "sink" we push to
	// note that this automatically handles marshalling to JSON
	var output bytes.Buffer
	jSink := jsonSink.New(&output) // gopkg.in/Clever/optimus.v3/sinks/json

	// let's set up some counting variables for fun
	beforeCounter := 0
	afterCounter := 0

	// selectOnlyCleverSSL will only output entries where ssl is true
	// and the host is either clever.com or a subdomain
	// if SSL or URL are missing, don't bother erroring and instead drop the row
	selectOnlyCleverSSL := func(row optimus.Row) (bool, error) {
		rawURL, ok := row["url"]
		if !ok {
			return false, nil
		}
		parsedURL, err := url.Parse(rawURL.(string))
		if err != nil || !regexp.MustCompile(`(\w*\.)?clever.com`).MatchString(parsedURL.Host) {
			return false, nil
		}

		// yes, yes, you could parse it from the protocol, but this is an example!
		ssl, err := strconv.ParseBool(row["ssl"].(string))
		if err != nil || ssl == false {
			return false, nil
		}

		return true, nil
	}

	// addProtocol prepends the URL with "https://" if SSL=true, else "http://" if false
	// also removes the SSL item from the row
	addProtocol := func(row optimus.Row) (optimus.Row, error) {
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

	// set up and start the transform
	err := New(cSource).
		// start by counting how many we read
		Each(func(r optimus.Row) error {
		beforeCounter++
		return nil
	}).
		// as a simple case, let's only match "clever.com" hosts with SSL true
		Select(selectOnlyCleverSSL).
		// then let's just append "https" onto those urls
		Map(addProtocol).
		// finish by counting how many we end up writing
		Each(func(r optimus.Row) error {
		afterCounter++
		return nil
	}).
		// set up the sink
		Sink(jSink)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(output.String())
		fmt.Printf("Done processing, ingested %d items, wrote %d items out.", beforeCounter, afterCounter)
	}

	// Output:
	// {"count":"5","url":"https://https://clever.com/foo"}
	// {"count":"2","url":"https://https://api.clever.com/bar"}
	//
	// Done processing, ingested 5 items, wrote 2 items out.
}

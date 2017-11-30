package main

import (
	"cb"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func parseConnectionParameters() (*cb.CouchbaseParams, error) {

	paramURI := "couchbaseUri"
	paramBucket := "bucket"
	paramBucketPwd := "bucketPassword"
	paramView := "view"
	paramStartDate := "start"
	paramEndDate := "end"

	cbURI := flag.String(paramURI, "couchbase://localhost", "the couchbase connection uri")
	cbBucket := flag.String(paramBucket, "", "couchbase bucket")
	cbBucketPwd := flag.String(paramBucketPwd, "", "couchbase bucket password")
	cbView := flag.String(paramView, "", "the reindexer view name")
	cbDevView := flag.Bool("dev", false, "use development view")
	cbLimit := flag.Uint("limit", 1000, "limit the number of results")
	cbSkip := flag.Uint("skip", 0, "start from this row")
	cbStart := flag.String(paramStartDate, "", "Starting date in the format YYYYMMDD")
	cbEnd := flag.String(paramEndDate, "", "Ending date in the format YYYYMMDD")
	cbPrefix := flag.String("prefix", "mutation", "Dump a prefix in front of ids")

	flag.Parse()

	if *cbURI == "" {
		return nil, errors.New("missing " + paramURI + " parameter")
	}

	if *cbBucket == "" {
		return nil, errors.New("missing " + paramBucket + " parameter")
	}

	if *cbView == "" {
		return nil, errors.New("missing " + paramView + " parameter")
	}

	var startTime *time.Time
	if *cbStart != "" {
		ts, err := time.Parse("20060102", *cbStart)
		if err != nil {
			return nil, errors.New("cannot parse " + paramStartDate + " parameter: " + err.Error())
		}
		startTime = &ts
	}

	var endTime *time.Time
	if *cbEnd != "" {
		ts, err := time.Parse("20060102", *cbEnd)
		if err != nil {
			return nil, errors.New("cannot parse " + paramEndDate + " parameter: " + err.Error())
		}
		endTime = &ts
	}

	idx := strings.Index(*cbView, ":")
	if (idx < 0) || (idx == len(*cbView)) {
		return nil, errors.New("The parameter " + paramView + " must be in the format \"designdocname:viewname\"")
	}

	cbDesignName := (*cbView)[0:idx]
	cbViewName := (*cbView)[idx+1:]

	return &cb.CouchbaseParams{
		Uri:        *cbURI,
		BucketName: *cbBucket,
		BucketPwd:  *cbBucketPwd,
		DesignName: cbDesignName,
		ViewName:   cbViewName,
		Params: cb.CouchbaseQueryParams{
			Development: *cbDevView,
			Limit:       *cbLimit,
			Skip:        *cbSkip,
			StartTime:   startTime,
			EndTime:     endTime,
			Prefix:      *cbPrefix,
		},
	}, nil
}

func usage(msg string) {
	fmt.Fprintf(os.Stderr, "%s\n\n", msg)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func getParamsOrUsage() *cb.CouchbaseParams {
	params, err := parseConnectionParameters()
	if err != nil {
		usage(err.Error())
	}
	return params
}

func main() {
	params := getParamsOrUsage()

	q, err := params.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting: %s\n", err.Error())
	}

	f := func(m map[string]interface{}) bool {
		if params.Params.Prefix != "" {
			fmt.Printf("%s:%s\n", params.Params.Prefix, m["value"])
		} else {
			fmt.Printf("%s\n", m["value"])
		}
		return true
	}

	counter, _ := q.Query(&f)

	if params.Params.Limit > 0 {
		fmt.Fprintf(os.Stderr, "For next batch run with -skip=%d\n", params.Params.Skip+counter)
	}

}

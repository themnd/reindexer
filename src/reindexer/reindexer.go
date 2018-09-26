package main

import (
	"cb"
	"errors"
	"flag"
	"fmt"
	"os"
	"solr"
	"strings"
	"time"
)

type programParams struct {
	cb   *cb.CouchbaseParams
	solr *solr.SolrConnectionParams
}

func parseConnectionParameters() (*programParams, error) {

	paramURI := "couchbaseUri"
	paramBucket := "bucket"
	paramBucketPwd := "bucketPassword"
	paramView := "view"
	paramStartDate := "start"
	paramEndDate := "end"
	paramSolrHost := "solrHost"
	paramSolrPort := "solrPort"

	cbURI := flag.String(paramURI, "", "the couchbase connection uri (such as couchbase://localhost)")
	cbBucket := flag.String(paramBucket, "", "couchbase bucket")
	cbBucketPwd := flag.String(paramBucketPwd, "", "couchbase bucket password")
	cbView := flag.String(paramView, "", "the reindexer view name")
	cbDevView := flag.Bool("dev", false, "use development view")
	cbLimit := flag.Uint("limit", 1000, "limit the number of results")
	cbSkip := flag.Uint("skip", 0, "start from this row")
	cbStart := flag.String(paramStartDate, "", "Starting date in the format YYYYMMDD")
	cbEnd := flag.String(paramEndDate, "", "Ending date in the format YYYYMMDD")
	cbPrefix := flag.String("prefix", "mutation", "Dump a prefix in front of ids")
	solrHost := flag.String(paramSolrHost, "", "Solr hostname")
	solrPort := flag.Uint(paramSolrPort, 8983, "Solr port")
	solrIndex := flag.String("solrIndex", "onecms", "Solr index name")
	useUnversioned := flag.Bool("unversioned", false, "output unversioned ids")

	if *cbPrefix == "deletion" && !*useUnversioned {
		fmt.Fprintf(os.Stderr, "deletion mutation expect unversioned ids, use -unversioned\n")
	}

	flag.Parse()

	var cbDesignName string
	var cbViewName string

	if (*solrHost == "") && (*cbURI == "") {
		return nil, errors.New("missing " + paramURI + " or " + paramSolrHost + " parameter")
	}

	if *cbURI != "" {
		if *cbBucket == "" {
			return nil, errors.New("missing " + paramBucket + " parameter")
		}

		if *cbView == "" {
			return nil, errors.New("missing " + paramView + " parameter")
		}

		idx := strings.Index(*cbView, ":")
		if (idx < 0) || (idx == len(*cbView)) {
			return nil, errors.New("The parameter " + paramView + " must be in the format \"designdocname:viewname\"")
		}

		cbDesignName = (*cbView)[0:idx]
		cbViewName = (*cbView)[idx+1:]
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

	var cbPtr *cb.CouchbaseParams
	var solrPtr *solr.SolrConnectionParams

	if *cbURI != "" {
		cbPtr = &cb.CouchbaseParams{
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
				Unversioned: *useUnversioned,
			},
		}
	}
	if *solrHost != "" {
		solrPtr = solr.NewSolrConnectionParams(*solrHost, *solrPort, *solrIndex)
		(*solrPtr).Params = solr.SolrQueryParams{
			Limit:     *cbLimit,
			Skip:      *cbSkip,
			StartTime: startTime,
			EndTime:   endTime,
			Prefix:    *cbPrefix,
		}
	}
	return &programParams{
		cb:   cbPtr,
		solr: solrPtr,
	}, nil
}

func usage(msg string) {
	fmt.Fprintf(os.Stderr, "%s\n\n", msg)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func getParamsOrUsage() *programParams {
	params, err := parseConnectionParameters()
	if err != nil {
		usage(err.Error())
	}
	return params
}

func queryCouchbase(params *cb.CouchbaseParams) {
	q, err := params.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting: %s\n", err.Error())
		return
	}

	f := func(m map[string]interface{}) bool {
		var value string
		value = m["value"].(string)
		offset := 7
		if params.Params.Unversioned {
			idx := offset + strings.Index(value[offset:], ":")
			if idx > offset {
				value = value[0:idx]
			}
		}
		if params.Params.Prefix != "" {
			fmt.Printf("%s:%s\n", params.Params.Prefix, value)
		} else {
			fmt.Printf("%s\n", value)
		}
		return true
	}

	counter, _ := q.Query(&f)

	if params.Params.Limit > 0 {
		fmt.Fprintf(os.Stderr, "For next batch run with -skip=%d\n", params.Params.Skip+counter)
	}
}

func querySolr(solr *solr.SolrConnectionParams) {
	conn, err := solr.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting: %s\n", err.Error())
		return
	}

	prefix := solr.Params.Prefix

	f := func(m map[string]interface{}) bool {
		if prefix != "" {
			fmt.Printf("%s:%s\n", prefix, m["id"])
		} else {
			fmt.Printf("%s\n", m["id"])
		}
		return true
	}

	_, err2 := conn.Query("id:onecms*", &f)
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "Error querying: %s\n", err2.Error())
		return
	}
}

func main() {
	version := "1.0.0"

	fmt.Fprintf(os.Stderr, "Version %s\n", version)

	params := getParamsOrUsage()

	if params.cb != nil {
		queryCouchbase(params.cb)
	}
	if params.solr != nil {
		querySolr(params.solr)
	}

}

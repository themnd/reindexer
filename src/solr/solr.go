package solr

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	solr "github.com/rtt/Go-Solr"
)

type SolrQueryParams struct {
	Limit     uint
	Skip      uint
	StartTime *time.Time
	EndTime   *time.Time
	Prefix    string
}

type SolrConnectionParams struct {
	host   string
	port   uint
	index  string
	Params SolrQueryParams
}

type SolrConnection struct {
	server *solr.Connection
	params SolrQueryParams
}

func NewSolrConnectionParams(host string, port uint, index string) *SolrConnectionParams {
	return &SolrConnectionParams{
		host:  host,
		port:  port,
		index: index,
	}
}

func (params *SolrConnectionParams) Connect() (*SolrConnection, error) {
	fmt.Fprintf(os.Stderr, "Connecting to %s:%d\n", params.host, params.port)

	s, err := solr.Init(params.host, int(params.port), params.index)

	if err != nil {
		return nil, errors.New("Error connecting to " + params.host + ":" + string(params.port) + " : " + err.Error())
	}

	conn := SolrConnection{
		server: s,
		params: params.Params,
	}

	return &conn, nil
}

func (conn *SolrConnection) Query(query string, s *func(map[string]interface{}) bool) (*solr.DocumentCollection, error) {
	q := solr.Query{
		Params: solr.URLParamMap{
			"q": []string{query},
		},
		Start: int(conn.params.Skip),
	}
	if conn.params.Limit > 0 {
		q.Rows = int(conn.params.Limit)
	} else {
		q.Rows = math.MaxInt32
	}
	if conn.params.StartTime != nil || conn.params.EndTime != nil {
		var start = "*"
		var end = "NOW"
		if conn.params.StartTime != nil {
			start = conn.params.StartTime.Format("2006-01-02T15:04:05Z")
		}
		if conn.params.EndTime != nil {
			end = conn.params.StartTime.Format("2006-01-02T15:04:05Z")
		}
		q.Params["fq"] = []string{"modificationTime:[" + start + " TO " + end + "]"}
	}

	// perform the query, checking for errors
	res, err := conn.server.Select(&q)
	if err != nil {
		return nil, errors.New("Error querying for " + query + " : " + err.Error())
	}

	fmt.Fprintf(os.Stderr, "Found %d results\n", res.Results.NumFound)

	docs := res.Results.Collection
	for idx := 0; idx < len(docs); idx++ {
		d := docs[idx]
		if s != nil {
			if !(*s)(d.Doc()) {
				break
			}
		}
	}

	return res.Results, nil
}

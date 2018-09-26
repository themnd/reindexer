package cb

import (
	"errors"
	"fmt"
	"os"
	"time"

	gocb "gopkg.in/couchbase/gocb.v1"
)

type CouchbaseQueryParams struct {
	Development bool
	Limit       uint
	Skip        uint
	StartTime   *time.Time
	EndTime     *time.Time
	Prefix      string
	Unversioned bool
}

type CouchbaseParams struct {
	Uri        string
	BucketName string
	BucketPwd  string
	DesignName string
	ViewName   string
	Params     CouchbaseQueryParams
}

type CouchbaseQuery struct {
	bucket *gocb.Bucket
	view   *gocb.ViewQuery
	params *CouchbaseQueryParams
}

func (params *CouchbaseParams) Connect() (*CouchbaseQuery, error) {
	fmt.Fprintf(os.Stderr, "Connecting to %s\n", params.Uri)

	cluster, err := gocb.Connect(params.Uri)
	if err != nil {
		return nil, errors.New("Error connecting to " + params.Uri + ": " + err.Error())
	}
	bucket, err := cluster.OpenBucket(params.BucketName, params.BucketPwd)
	if err != nil {
		return nil, errors.New("Error opening bucket " + params.BucketName + ": " + err.Error())
	}

	fmt.Fprintf(os.Stderr, "Connected to %s\n", bucket.Name())

	cq := CouchbaseQuery{
		bucket: bucket,
		view:   gocb.NewViewQuery(params.DesignName, params.ViewName),
		params: &params.Params,
	}
	return &cq, nil
}

func (cq *CouchbaseQuery) Query(s *func(map[string]interface{}) bool) (uint, error) {
	if cq.params.Development {
		fmt.Fprintf(os.Stderr, "Start development query\n")
	} else {
		fmt.Fprintf(os.Stderr, "Start query\n")
	}
	view := cq.view
	view.Development(cq.params.Development)
	view.Stale(gocb.Before)
	if cq.params.Limit > 0 {
		view.Limit(cq.params.Limit)
	}
	view.Skip(cq.params.Skip)
	view.Order(gocb.Ascending)
	if cq.params.StartTime != nil || cq.params.EndTime != nil {
		var start interface{}
		var end interface{}
		if cq.params.StartTime != nil {
			var a [3]int
			a[0] = cq.params.StartTime.Year()
			a[1] = int(cq.params.StartTime.Month())
			a[2] = cq.params.StartTime.Day()
			start = a
		}
		if cq.params.EndTime != nil {
			var a [3]int
			a[0] = cq.params.EndTime.Year()
			a[1] = int(cq.params.EndTime.Month())
			a[2] = cq.params.EndTime.Day()
			end = a
		}
		view.Range(start, end, true)
	}

	rows, err := cq.bucket.ExecuteViewQuery(view)
	if err != nil {
		viewBody := `
function (doc, meta) {
	if (meta.id.indexOf('HangerInfo::') == 0) {
		if (doc.versions && doc.versions.length > 0) {
			var lastVersion = doc.versions[doc.versions.length - 1];
			var ts = dateToArray(lastVersion.creationInfo.timestamp).slice(0, 3);
			emit(ts, lastVersion.version);
		}
	}
}
		`
		fmt.Fprintf(os.Stderr, "error during query %s\nif the view does not exists you can create it with\n%s", err.Error(), viewBody)
		return 0, err
	}

	var row interface{}
	var counter uint
	for rows.Next(&row) {
		counter++
		m := row.(map[string]interface{})
		if s != nil {
			if !(*s)(m) {
				break
			}
		}
	}

	return counter, nil
}

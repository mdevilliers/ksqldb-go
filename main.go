package main

import (
	"fmt"

	ksqldb "github.com/mdevilliers/ksqldb-go/pkg/client"
)

func main() {

	url := "http://0.0.0.0:8088"

	client, err := ksqldb.New(url)

	if err != nil {
		panic(err)
	}

	info, err := client.ServerInfo()

	if err != nil {
		panic(err)
	}
	fmt.Println(info)

	healthCheck, err := client.ServerHealthCheck()

	if err != nil {

		if !ksqldb.IsClientError(err) {
			panic(err)
		}

		//	cErr, ok := err.(*ksqldb.Error)
		//	fmt.Println(cErr.StackTrace, ok)

	}

	fmt.Println(healthCheck, err)

	result, err := client.Command(ksqldb.Statement{
		KSQL: `CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');`})

	if err != nil {
		if !ksqldb.IsClientError(err) {
			panic(err)
		}
		//		err2, ok := err.(*ksqldb.StatementError)
		//		fmt.Println(err2.StatementText, ok)
	}
	fmt.Println(result, err)

	result, err = client.Command(ksqldb.Statement{
		KSQL: `CREATE TABLE users_original (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR) WITH
		   (kafka_topic='users', value_format='JSON', key = 'userid');`})

	if err != nil {
		if !ksqldb.IsClientError(err) {
			panic(err)
		}
		//		err2, ok := err.(*ksqldb.StatementError)
		//		fmt.Println(err2.StatementText, ok)
	}

	topics, err := client.ListTopics()

	if err != nil {
		panic(err)
	}

	fmt.Println("topics", topics)

	streams, err := client.ListStreams()

	if err != nil {
		panic(err)
	}

	fmt.Println("streams", streams)

	tables, err := client.ListTables()

	if err != nil {
		panic(err)
	}

	fmt.Println("tables", tables)

	queries, err := client.ListQueries()

	if err != nil {
		panic(err)
	}

	fmt.Println("queries", queries)

	props, err := client.ListProperties()

	if err != nil {
		panic(err)
	}

	fmt.Println("props", props)

	out := make(chan ksqldb.QueryResponse, 10)

	go func() {
		for {
			select {
			case r := <-out:
				fmt.Println(r)
			}

		}
	}()

	result, err = client.Command(ksqldb.Statement{
		KSQL: `CREATE STREAM pageviews_enriched AS
		       SELECT users_original.userid AS userid, pageid, regionid, gender
		       FROM pageviews_original
		       LEFT JOIN users_original
		       ON pageviews_original.userid = users_original.userid ;`})

	if err != nil {
		if !ksqldb.IsClientError(err) {
			panic(err)
		}
		//		err2, ok := err.(*ksqldb.StatementError)
		//		fmt.Println(err2.StatementText, ok)
	}

	err = client.Query(ksqldb.QueryRequest{KSQL: `SELECT pageid FROM pageviews_original LIMIT 3;`}, out)

	if err != nil {
		panic(err)
	}

	err = client.Query(ksqldb.QueryRequest{KSQL: `SELECT * FROM pageviews_enriched;`}, out)
	//map[string]string{	"ksql.streams.auto.offset.reset": "earliest",}
	if err != nil {
		panic(err)
	}

}

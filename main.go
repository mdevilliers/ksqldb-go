package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/hashicorp/go-cleanhttp"
)

func main() {

	url := "http://0.0.0.0:8088"

	client, err := NewClient(url)

	if err != nil {
		panic(err)
	}
	err = client.Statement(`SHOW TOPICS;`,
		map[string]string{})
	if err != nil {
		panic(err)
	}

	err = client.Statement(`CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH
	   (kafka_topic='pageviews', value_format='DELIMITED');`, nil)

	if err != nil {
		panic(err)
	}

	err = client.Statement(`CREATE TABLE users_original (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR) WITH
	   (kafka_topic='users', value_format='JSON', key = 'userid');`, nil)

	if err != nil {
		panic(err)
	}
	err = client.Statement(`SHOW STREAMS;`,
		map[string]string{})
	if err != nil {
		panic(err)
	}
	err = client.Statement(`SHOW TABLES;`,
		map[string]string{})
	if err != nil {
		panic(err)
	}
	out := make(chan SelectResponse, 10)

	go func() {
		for {
			select {
			case r := <-out:
				fmt.Println(r)
			}

		}
	}()

	err = client.Statement(`CREATE STREAM pageviews_enriched AS
	       SELECT users_original.userid AS userid, pageid, regionid, gender
	       FROM pageviews_original
	       LEFT JOIN users_original
	       ON pageviews_original.userid = users_original.userid ;`,
		nil)

	if err != nil {
		panic(err)
	}

	//	err = client.Select(`SELECT pageid FROM pageviews_original LIMIT 3;`, nil, out)

	//	if err != nil {
	//		panic(err)
	//	}
	err = client.Select(`SELECT * FROM pageviews_enriched;`, map[string]string{
		//	"ksql.streams.auto.offset.reset": "earliest",
	}, out)

	if err != nil {
		panic(err)
	}

}

type client struct {
	Address    string
	HTTPClient *http.Client
	// url is the parsed URL from Address
	url *url.URL
}

// TODO : add ability to set httpclient
func NewClient(address string) (*client, error) {

	c := &client{
		Address: address,
	}
	u, err := url.Parse(c.Address)
	if err != nil {
		return nil, err
	}
	c.url = u

	if c.HTTPClient == nil {
		c.HTTPClient = cleanhttp.DefaultClient()
	}

	return c, nil
}

type selectQuery struct {
	KSQL       string            `json:"ksql"`
	Properties map[string]string `json:"streamsProperties,omitempty"`
}

type statementQuery struct {
	KSQL                  string            `json:"ksql"`
	Properties            map[string]string `json:"streamsProperties,omitempty"`
	CommandSequenceNumber uint64            `json:"commandSequenceNumber,omitempty"`
}

const ContentType = "application/vnd.ksql.v1+json; charset=utf-8"

func (c *client) Statement(statement string /*sequenceNumber uint64,*/, properties map[string]string) error {
	uri := "/ksql"

	b := &statementQuery{
		KSQL: statement,
		//	CommandSequenceNumber: sequenceNumber,
		Properties: properties,
	}
	resp, err := c.doRequest(uri, b)

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)

	bytes, err := ioutil.ReadAll(r)

	if err != nil {
		return err
	}
	fmt.Println(string(bytes))

	return nil
}

func (c *client) doRequest(uri string, b interface{}) (*http.Response, error) {

	content, err := json.Marshal(b)

	if err != nil {
		return nil, err
	}

	u := strings.TrimRight(c.url.String(), "/") + "/" + strings.TrimLeft(uri, "/")
	req, err := http.NewRequest("POST", u, bytes.NewBuffer(content))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", ContentType)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Accept", "*/*")

	return c.HTTPClient.Do(req)
}

func (c *client) Select(query string, properties map[string]string, out chan SelectResponse) error {

	uri := "/query"

	b := &selectQuery{
		KSQL:       query,
		Properties: properties,
	}

	resp, err := c.doRequest(uri, b)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	for {

		line, err := reader.ReadBytes('\n')

		if err != nil {

			if err == io.EOF {
				return nil
			}

			return err
		}
		if len(line) > 1 {
			r := SelectResponse{}
			err = json.Unmarshal(line, &r)

			if err != nil {
				return err
			}
			out <- r
		}
	}

	return nil
}

type SelectResponse struct {
	Row          Row    `json:"row"`
	ErrorMessage string `json:"errorMessage"`
	FinalMessage string `json:"finalMessage"`
	Terminal     bool   `json:"terminal"`
}

type Row struct {
	Columns []interface{} `json:"columns"`
}

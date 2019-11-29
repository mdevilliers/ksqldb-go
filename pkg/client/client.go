package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/pkg/errors"
)

const ContentType = "application/vnd.ksql.v1+json; charset=utf-8"

type client struct {
	Address    string
	httpClient *http.Client
	// url is the parsed URL from Address
	url *url.URL
}

type Option func(c *client) error

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *client) error {
		c.httpClient = httpClient
		return nil
	}
}

func New(address string, options ...Option) (*client, error) {

	c := &client{
		Address: address,
	}

	u, err := url.Parse(c.Address)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing 'address'")
	}
	c.url = u

	for _, o := range options {
		err := o(c)
		if err != nil {
			return nil, errors.Wrap(err, "error applying option")
		}
	}
	if c.httpClient == nil {
		c.httpClient = cleanhttp.DefaultClient()
	}

	return c, nil
}

type StatementQuery struct {
	KSQL                  string            `json:"ksql"`
	Properties            map[string]string `json:"streamsProperties,omitempty"`
	CommandSequenceNumber uint64            `json:"commandSequenceNumber,omitempty"`
}

func (c *client) Statement(s StatementQuery, out interface{}) error {

	resp, err := c.doPost("/ksql", s)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	r := bufio.NewReader(resp.Body)

	bytes, err := ioutil.ReadAll(r)

	if err != nil {
		return err
	}

	if resp.StatusCode > http.StatusAccepted {
		e := &Error{}
		err = json.Unmarshal(bytes, e)

		if err != nil {
			return errors.Wrap(err, "error unmarshalling error ")
		}

		return e
	}

	fmt.Println(string(bytes))
	return json.Unmarshal(bytes, out)
}

func (c *client) doGet(uri string, out interface{}) error {

	u := strings.TrimRight(c.url.String(), "/") + "/" + strings.TrimLeft(uri, "/")

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	r := bufio.NewReader(resp.Body)

	bytes, err := ioutil.ReadAll(r)

	if err != nil {
		return err
	}

	if resp.StatusCode > http.StatusAccepted {
		e := &Error{}
		err = json.Unmarshal(bytes, e)

		if err != nil {
			return errors.Wrap(err, "error unmarshalling error ")
		}

		return e
	}
	return json.Unmarshal(bytes, out)
}

func (c *client) doPost(uri string, b interface{}) (*http.Response, error) {

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

	// IMPORTANT : if not set KSQLDB will periodically flush message rather
	// than push message back one at a time when ins streaming mode
	// TODO : make a configration option
	req.Header.Set("Accept-Encoding", "identity")

	req.Header.Set("Accept", "*/*")

	return c.httpClient.Do(req)
}

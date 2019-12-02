package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/pkg/errors"
)

const ContentType = "application/vnd.ksql.v1+json; charset=utf-8"

type Client struct {
	Address    string
	httpClient *http.Client
	// url is the parsed URL from Address
	url *url.URL
}

type Option func(c *Client) error

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) error {
		c.httpClient = httpClient
		return nil
	}
}

func New(address string, options ...Option) (*Client, error) {

	c := &Client{
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

func (c *Client) doGet(uri string, out interface{}) error {

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

func (c *Client) doPost(uri string, b interface{}) (*http.Response, error) {

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

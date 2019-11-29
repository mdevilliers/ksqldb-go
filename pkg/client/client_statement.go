package client

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

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

	//	fmt.Println(string(bytes))
	return json.Unmarshal(bytes, out)
}

package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

type Statement struct {
	KSQL           string            `json:"ksql"`
	Properties     map[string]string `json:"streamsProperties,omitempty"`
	SequenceNumber uint64            `json:"commandSequenceNumber,omitempty"`
}

type CommandResponse struct {
	StatementText  string        `json:"statementText"`
	Warnings       []string      `json:"warnings"`
	CommandID      string        `json:"commandId"`
	Status         CommandStatus `json:"commandStatus"`
	SequenceNumber int64         `json:"commandSequenceNumber"`
}

type CommandStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (c *client) Command(s Statement) ([]CommandResponse, error) {

	r := []CommandResponse{}
	err := c.Statement(s, &r)
	return r, err
}

func (c *client) Statement(s Statement, out interface{}) error {

	resp, err := c.doPost("/ksql", s)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	r := bufio.NewReader(resp.Body)

	bytes, err := ioutil.ReadAll(r)
	fmt.Println(string(bytes))

	if err != nil {
		return err
	}

	if resp.StatusCode > http.StatusAccepted {
		var err error
		e := &unknownJSON{}
		err = json.Unmarshal(bytes, e)

		if err != nil {
			return errors.Wrap(err, "error unmarshalling error ")
		}

		switch e.Type {
		case "statement_error":
			err = &StatementError{}
		default:
			err = &Error{}

		}
		// OK to ignore error I think - should be valid JSON
		_ = json.Unmarshal(bytes, err)
		return err
	}

	return json.Unmarshal(bytes, out)
}

type unknownJSON struct {
	Type string `json:"@type"`
}

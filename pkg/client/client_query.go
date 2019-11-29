package client

import (
	"bufio"
	"encoding/json"
	"io"
)

type QueryRequest struct {
	KSQL       string            `json:"ksql"`
	Properties map[string]string `json:"streamsProperties,omitempty"`
}

type QueryResponse struct {
	Row          Row    `json:"row"`
	ErrorMessage string `json:"errorMessage"`
	FinalMessage string `json:"finalMessage"`
	Terminal     bool   `json:"terminal"`
}

type Row struct {
	Columns []interface{} `json:"columns"`
}

func (c *client) Query(q QueryRequest, out chan QueryResponse) error {

	resp, err := c.doPost("/query", q)

	if err != nil {
		// TODO : handle errors?
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	for {

		line, err := reader.ReadBytes('\n')

		if err != nil {

			if err == io.EOF {
				break
			}

			return err
		}
		if len(line) > 1 {
			r := QueryResponse{}
			err = json.Unmarshal(line, &r)

			if err != nil {
				return err
			}
			out <- r
		}
	}

	return nil
}

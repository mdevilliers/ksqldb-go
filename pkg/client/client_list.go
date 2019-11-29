package client

import "errors"

type Queries struct {
	StatementText string   `json:"statementText"`
	Warnings      []string `json:"warnings"`
	Queries       []Query  `json:"queries"`
}

type Query struct {
	QueryString string   `json:"queryString"`
	Sinks       []string `json:"sinks"`
	ID          string   `json:"id"`
}

type Topics struct {
	StatementText string   `json:"statementText"`
	Warnings      []string `json:"warnings"`
	Topics        []Topic  `json:"topics"`
}

type Topic struct {
	Name               string `json:"name"`
	Registerd          bool   `json:"registerd"`
	ReplicaInfo        []int  `json:"replicaInfo"`
	ConsumerCount      int    `json:"consumerCount"`
	ConsumerGroupCount int    `json:"consumerGroupCount"`
}

type Streams struct {
	StatementText string   `json:"statementText"`
	Warnings      []string `json:"warnings"`
	Streams       []Stream `json:"streams"`
}

type Stream struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Topic  string `json:"topic"`
	Format string `json:"format"`
}

type Properties struct {
	StatementText         string            `json:"statementText"`
	Warnings              []string          `json:"warnings"`
	Properties            map[string]string `json:"properties"`
	OverwrittenProperties []string          `json:"overwrittenProperties"`
	DefaultProperties     []string          `json:"defaultProperties"`
}

type Tables struct {
	StatementText string   `json:"statementText"`
	Warnings      []string `json:"warnings"`
	Tables        []Table  `json:"tables"`
}

type Table struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Topic      string `json:"topic"`
	Format     string `json:"format"`
	IsWindowed bool   `json:"isWindowed"`
}

func (c *client) ListStreams() (Streams, error) {

	r := []Streams{}
	err := c.Statement(Statement{KSQL: "LIST STREAMS;"}, &r)
	if err != nil {
		return Streams{}, err
	}
	if len(r) != 1 {
		return Streams{}, errors.New("error deserialising 'Streams' was only expecting 1")
	}

	return r[0], err
}

func (c *client) ListTables() (Tables, error) {

	r := []Tables{}
	err := c.Statement(Statement{KSQL: "LIST TABLES;"}, &r)
	if err != nil {
		return Tables{}, err
	}
	if len(r) != 1 {
		return Tables{}, errors.New("error deserialising 'Tables' was only expecting 1")
	}

	return r[0], err
}
func (c *client) ListTopics() (Topics, error) {

	r := []Topics{}
	err := c.Statement(Statement{KSQL: "LIST TOPICS;"}, &r)
	if err != nil {
		return Topics{}, err
	}
	if len(r) != 1 {
		return Topics{}, errors.New("error deserialising 'Topics' was only expecting 1")
	}

	return r[0], err
}

func (c *client) ListQueries() (Queries, error) {

	r := []Queries{}
	err := c.Statement(Statement{KSQL: "LIST QUERIES;"}, &r)
	if err != nil {
		return Queries{}, err
	}
	if len(r) != 1 {
		return Queries{}, errors.New("error deserialising 'Queries' was only expecting 1")
	}

	return r[0], err
}

func (c *client) ListProperties() (Properties, error) {

	r := []Properties{}
	err := c.Statement(Statement{KSQL: "LIST PROPERTIES;"}, &r)
	if err != nil {
		return Properties{}, err
	}
	if len(r) != 1 {
		return Properties{}, errors.New("error deserialising 'Properties' was only expecting 1")
	}

	return r[0], err
}

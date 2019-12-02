package client

func (c *Client) ServerInfo() (ServerInfo, error) {

	r := serverInfoResponse{}
	err := c.doGet("/info", &r)
	if err != nil {
		return ServerInfo{}, err
	}
	return r.KSQLServerInfo, nil
}

type serverInfoResponse struct {
	KSQLServerInfo ServerInfo `json:"ksqlServerInfo"`
}

type ServerInfo struct {
	Version        string `json:"version"`
	KafkaClusterID string `json:"kafkaClusterId"`
	KSQLServiceID  string `json:"ksqlServiceId"`
}

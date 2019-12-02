package client

func (c *Client) ServerHealthCheck() (HealthCheck, error) {

	r := HealthCheck{Details: map[string]HealthCheckDetail{}}
	err := c.doGet("/healthcheck", &r)
	return r, err
}

type HealthCheck struct {
	HealthCheckDetail
	Details map[string]HealthCheckDetail `json:"details"`
}

type HealthCheckDetail struct {
	IsHealthy bool `json:"isHealthy"`
}

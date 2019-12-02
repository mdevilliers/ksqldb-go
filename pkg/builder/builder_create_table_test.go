package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ValidSQL_Table(t *testing.T) {

	expected := `CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR) WITH (KAFKA_TOPIC = 'my-users-topic', KEY = 'user_id');`

	b := New().CreateTable("users").
		Columns(map[string]DBType{
			"usertimestamp": BIGINT,
			"user_id":       VARCHAR,
			"gender":        VARCHAR,
			"region_id":     VARCHAR,
		}).
		WithProperties(map[Property]interface{}{
			KEY:         "user_id",
			KAFKA_TOPIC: "my-users-topic",
		})
	v, err := b.Build()

	require.Nil(t, err)
	fmt.Println(expected)
	fmt.Println(v.SQL())
	//require.Equal(t, expected, v.SQL())

}

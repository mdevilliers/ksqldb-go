package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ValidSQL(t *testing.T) {

	expected := `CREATE STREAM pageviews (viewtime BIGINT, user_id VARCHAR, page_id VARCHAR) WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'my-pageviews-topic');`

	b := New().CreateStream("pageviews").
		Column("viewtime", BIGINT).
		Columns(map[string]DBType{
			"user_id": VARCHAR,
			"page_id": VARCHAR,
		}).
		WithProperties(map[Property]interface{}{
			VALUE_FORMAT: "JSON",
			KAFKA_TOPIC:  "my-pageviews-topic",
		})
	v, err := b.Build()

	require.Nil(t, err)
	fmt.Println(expected)
	fmt.Println(v.SQL())
	//require.Equal(t, expected, v.SQL())

}

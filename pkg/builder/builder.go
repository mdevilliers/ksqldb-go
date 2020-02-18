package builder

import (
	"fmt"
	"strings"
)

type DBType string
type Property string

var (
	BOOLEAN = DBType("BOOLEAN")
	INTEGER = DBType("INTEGER")
	INT     = DBType("INT")
	BIGINT  = DBType("BIGINT")
	DOUBLE  = DBType("DOUBLE")
	VARCHAR = DBType("VARCHAR")
	STRING  = DBType("STRING")

	ARRAY_BOOLEAN = DBType("ARRAY<BOOLEAN>")
	ARRAY_INTEGER = DBType("ARRAY<INTEGER>")
	ARRAY_INT     = DBType("ARRAY<INT>")
	ARRAY_BIGINT  = DBType("ARRAY<BIGINT>")
	ARRAY_DOUBLE  = DBType("ARRAY<DOUBLE>")
	ARRAY_VARCHAR = DBType("ARRAY<VARCHAR>")
	ARRAY_STRING  = DBType("ARRAY<STRING>")

	MAP_BOOLEAN = DBType("MAP<STRING, BOOLEAN>")
	MAP_INTEGER = DBType("MAP<STRING, INTEGER>")
	MAP_INT     = DBType("MAP<STRING, INT>")
	MAP_BIGINT  = DBType("MAP<STRING, BIGINT>")
	MAP_DOUBLE  = DBType("MAP<STRING, DOUBLE>")
	MAP_VARCHAR = DBType("MAP<STRING, VARCHAR>")
	MAP_STRING  = DBType("MAP<STRING, STRING>")

	KAFKA_TOPIC       = Property("KAFKA_TOPIC")
	VALUE_FORMAT      = Property("VALUE_FORMAT")
	PARTITIONS        = Property("PARTITIONS")
	REPLICAS          = Property("REPLICAS")
	VALUE_DELIMITER   = Property("VALUE_DELIMITER")
	KEY               = Property("KEY")
	TIMESTAMP         = Property("TIMESTAMP")
	TIMESTAMP_FORMAT  = Property("TIMESTAMP_FORMAT")
	WRAP_SINGLE_VALUE = Property("WRAP_SINGLE_VALUE")
	WINDOW_TYPE       = Property("WINDOW_TYPE")
	WINDOW_SIZE       = Property("WINDOW_SIZE")
)

func Decimal(precision, scale int) DBType {
	return DBType(fmt.Sprintf("DECIMAL(%d, %d)", precision, scale))
}

func Map(key, value DBType) DBType {
	return DBType(fmt.Sprintf("MAP<%s, %s>", key, value))
}

type b struct {
}

func New() *b {
	return &b{}
}

type defineStruct struct{}

func (b *b) DefineStruct() *defineStruct {
	return &defineStruct{}
}

func (ds *defineStruct) WithProperty() *defineStruct {
	return ds
}

type create struct {
	entity   string
	name     string
	cols     map[string]DBType
	props    map[Property]interface{}
	selector *selector
}

func (b *b) CreateStream(name string) *create {
	return &create{
		entity: "STREAM",
		name:   name,
		cols:   map[string]DBType{},
		props:  map[Property]interface{}{},
	}
}

func (b *b) CreateTable(name string) *create {
	return &create{
		entity: "TABLE",
		name:   name,
		cols:   map[string]DBType{},
		props:  map[Property]interface{}{},
	}
}

func (cs *create) Column(name string, t DBType) *create {
	cs.cols[Escape(name)] = t
	return cs
}
func (cs *create) Columns(c map[string]DBType) *create {
	for k, v := range c {
		cs.cols[Escape(k)] = v
	}

	return cs
}
func (cs *create) WithProperty(key Property, value interface{}) *create {
	cs.props[key] = value
	return cs
}
func (cs *create) WithProperties(p map[Property]interface{}) *create {
	for k, v := range p {
		cs.props[k] = v
	}
	return cs
}

type selector struct {
	from string
}

func (cs *create) AsSelect() *selector {
	cs.selector = &selector{}
	return cs.selector
}

func (s *selector) From(from string) *selector {
	s.from = from
	return s
}

// e.g LeftJoin( "xx", "xx.foo = zz.bar" )
func (s *selector) LeftJoin(from, criteria string) *selector {
	return s
}
func (s *selector) FullJoin(from, criteria string) *selector {
	return s
}
func (s *selector) InnerJoin(from, criteria string) *selector {
	return s
}

func (s *selector) Where(condition string) *selector {
	return s
}

func (s *selector) Colummns() *selector {
	return s
}

type value struct {
	buf strings.Builder
}

func (cs *create) Build() (value, error) {

	buf := strings.Builder{}

	buf.WriteString("CREATE ")
	buf.WriteString(cs.entity)
	buf.WriteRune(' ')
	buf.WriteString(cs.name)

	if len(cs.cols) > 0 {
		buf.WriteString(" (")

		pairs := make([]string, 0, len(cs.cols))
		for k, v := range cs.cols {
			pairs = append(pairs, fmt.Sprintf("%s %s", k, v))
		}

		buf.WriteString(strings.Join(pairs, ", "))
		buf.WriteRune(')')
	}

	if len(cs.props) > 0 {

		buf.WriteString(" WITH (")

		pairs := make([]string, 0, len(cs.props))
		for k, v := range cs.props {

			switch v.(type) {
			case string:
				pairs = append(pairs, fmt.Sprintf("%s = '%s'", k, v))
			default:
				pairs = append(pairs, fmt.Sprintf("%s = %s", k, v))
			}

		}

		buf.WriteString(strings.Join(pairs, ", "))
		buf.WriteRune(')')
	}

	buf.WriteRune(';')

	return value{
		buf: buf,
	}, nil
}

func (v value) SQL() string {
	return v.buf.String()
}

func Escape(str string) string {
	return str
}

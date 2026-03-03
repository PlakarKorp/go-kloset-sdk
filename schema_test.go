package sdk

import (
	_ "embed"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

//go:embed test_schema.json
var test_schema string

//go:embed test_config.yaml
var test_config string

func TestSchema(t *testing.T) {
	schema, err := NewSchemaFromString(test_schema)
	require.NoError(t, err)

	config, err := parseYAML(test_config)
	require.NoError(t, err)

	obj := schema.TransformConfig(config)

	err = schema.Validate(obj)
	require.NoError(t, err)

	schema.ApplyDefaults(obj)

	require.Equal(t, "the-default-value", obj["some_key_with_default"])

	some_integer, err := strconv.ParseInt(config["some_integer"], 10, 64)
	require.NoError(t, err)
	require.Equal(t, some_integer, obj["some_integer"])

	some_number, err := strconv.ParseFloat(config["some_number"], 64)
	require.NoError(t, err)
	require.Equal(t, some_number, obj["some_number"])

	use_tls, err := strconv.ParseBool(config["use_tls"])
	require.NoError(t, err)
	require.Equal(t, use_tls, obj["use_tls"])
}

type TestConfig struct {
	Location            string  `json:"location"`
	AccessKey           string  `json:"access_key"`
	SecretAccessKey     string  `json:"secret_access_key"`
	UseTLS              bool    `json:"use_tls"`
	TLSInsecureNoVerify bool    `json:"tls_insecure_no_verify"`
	SomeInteger         int     `json:"some_integer"`
	SomeNumber          float64 `json:"some_number"`
	SomeKeyWithDefault  string  `json:"some_key_with_default"`
}

func TestDecodeSchema(t *testing.T) {
	config, err := parseYAML(test_config)
	require.NoError(t, err)

	var cfg TestConfig
	err = DecodeConfig(test_schema, config, &cfg)
	require.NoError(t, err)

	require.Equal(t, "xxx", cfg.SecretAccessKey)
	require.True(t, cfg.UseTLS)
	require.False(t, cfg.TLSInsecureNoVerify)
	require.Equal(t, -38, cfg.SomeInteger)
	require.Equal(t, 45.123, cfg.SomeNumber)
	require.Equal(t, "the-default-value", cfg.SomeKeyWithDefault)
}

func parseYAML(value string) (map[string]string, error) {
	var res map[string]string
	if err := yaml.Unmarshal([]byte(value), &res); err != nil {
		return nil, err
	}

	return res, nil
}

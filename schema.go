package sdk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type Schema struct {
	schema *jsonschema.Schema
	raw    map[string]any
}

func ValidateConfig(schema string, config map[string]string) (map[string]any, error) {
	s, err := NewSchemaFromString(schema)
	if err != nil {
		return nil, err
	}

	return s.ValidateConfig(config)
}

func DecodeConfig(schema string, config map[string]string, dst any) error {
	s, err := NewSchemaFromString(schema)
	if err != nil {
		return err
	}

	tc, err := s.ValidateConfig(config)
	if err != nil {
		return err
	}

	applyDefaults(tc, s.raw["properties"].(map[string]any))
	buf, err := json.Marshal(tc)
	if err != nil {
		return err
	}

	return json.Unmarshal(buf, dst)
}

func NewSchemaFromString(data string) (*Schema, error) {
	obj, err := jsonschema.UnmarshalJSON(strings.NewReader(data))
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return nil, err
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", obj); err != nil {
		return nil, err
	}

	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return nil, err
	}

	return &Schema{
		schema: schema,
		raw:    raw,
	}, nil
}

func (s *Schema) ValidateConfig(config map[string]string) (map[string]any, error) {
	tc, err := s.transformConfig(config)
	if err != nil {
		return nil, err
	}

	if err := s.schema.Validate(tc); err != nil {
		return nil, err
	}

	return tc, nil
}

func (schema *Schema) transformConfig(config map[string]string) (map[string]any, error) {
	out := make(map[string]any, len(config))
	for k, v := range config {
		switch v {
		//
		case "true", "false":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("invalid %s value", k)
			}
			out[k] = b
		default:
			out[k] = v
		}
	}
	return out, nil
}

// applyDefaults recursively applies defaults from schemaProps to obj
func applyDefaults(obj map[string]any, schemaProps map[string]any) {
	for key, v := range schemaProps {
		prop, ok := v.(map[string]any)
		if !ok {
			continue
		}
		// If instance missing and default present:
		if _, exists := obj[key]; !exists {
			if def, hasDefault := prop["default"]; hasDefault {
				obj[key] = def
				continue
			}
		}
		// Recurse for nested objects
		if nestedObj, isObj := obj[key].(map[string]any); isObj {
			if nestedProps, hasProps := prop["properties"].(map[string]any); hasProps {
				applyDefaults(nestedObj, nestedProps)
			}
		}
	}
}

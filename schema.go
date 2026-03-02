package sdk

import (
	"encoding/json"
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
	tc, err := s.TransformConfig(config)
	if err != nil {
		return nil, err
	}

	if err := s.schema.Validate(tc); err != nil {
		return nil, err
	}

	return tc, nil
}

func (schema *Schema) TransformConfig(config map[string]string) (map[string]any, error) {
	get_type_for_key := func(key string) (string, bool) {
		obj, found := schema.raw["properties"]
		if !found {
			return "", false
		}

		properties, ok := obj.(map[string]any)
		if !ok {
			return "", false
		}

		obj, found = properties[key]
		if !found {
			return "", false
		}

		property, ok := obj.(map[string]any)
		if !ok {
			return "", false
		}

		v, found := property["type"]
		if !found {
			return "", false
		}

		typ, ok := v.(string)
		return typ, ok
	}

	out := make(map[string]any, len(config))
	for k, v := range config {
		// Use the original value by default
		out[k] = v

		// If possible, parse value into the expected type as defined by the schema.
		// In case of error, just keep the original value and let validation fail later.
		if expectedType, found := get_type_for_key(k); found {
			switch expectedType {
			case "null":
			case "boolean":
				if b, err := strconv.ParseBool(v); err == nil {
					out[k] = b
				}
			case "object":
			case "array":
			case "number":
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					out[k] = f
				}
			case "string":
			case "integer":
				if strings.HasPrefix(v, "-") {
					if i, err := strconv.ParseInt(v, 10, 64); err == nil {
						out[k] = i
					}
				} else {
					if i, err := strconv.ParseUint(v, 10, 64); err == nil {
						out[k] = i
					}
				}
			default:
			}
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

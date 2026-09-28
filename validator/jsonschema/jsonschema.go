package jsonschema

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	js "github.com/santhosh-tekuri/jsonschema/v6"
)

// MaxSize caps how many bytes ReadAndValidate reads from its input.
const MaxSize = 1 << 20

var ErrTooLarge = errors.New("schema too large")

// schemaURL is only an identifier for the in-memory document; nothing is
// fetched from it.
const schemaURL = "file:///schema.json"

func ReadAndValidate(r io.Reader) error {
	data, err := io.ReadAll(io.LimitReader(r, MaxSize+1))
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	return validate(data)
}

// validate reads a JSON Schema document from r and checks it against the
// meta-schema named by its $schema keyword, draft 2020-12 if absent. External
// $ref targets are not loaded and make validation fail.
func validate(data []byte) error {
	if len(data) > MaxSize {
		return ErrTooLarge
	}

	doc, err := js.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("parse schema: %w", err)
	}

	c := js.NewCompiler()
	c.DefaultDraft(js.Draft2020)
	// The default loader reads file:// refs from disk. An empty scheme map
	// refuses every URL; meta-schemas are embedded and still resolve.
	c.UseLoader(js.SchemeURLLoader{})
	if err := c.AddResource(schemaURL, doc); err != nil {
		return fmt.Errorf("add schema: %w", err)
	}
	if _, err := c.Compile(schemaURL); err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}
	return nil
}

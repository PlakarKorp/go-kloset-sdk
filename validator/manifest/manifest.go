package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/PlakarKorp/pkg"
	"go.yaml.in/yaml/v3"
)

// MaxSize caps how many bytes ReadAndValidate reads from its input.
const MaxSize = 1 << 20

var (
	ErrTooLarge     = errors.New("manifest too large")
	ErrTrailingData = errors.New("trailing data after manifest")
	ErrNoConnectors = errors.New("no connectors")
)

// ReadAndValidate reads a plakar manifest.yaml from r and checks it.
//
// It is stricter than pkg.Manifest.Parse, which plakar uses to load
// manifests: unknown fields are rejected, since the loader silently drops
// them (a misspelled location_flags loses the flags), and name, display_name,
// description and each connector's protocols are required.
func ReadAndValidate(r io.Reader) error {
	data, err := io.ReadAll(io.LimitReader(r, MaxSize+1))
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if len(data) > MaxSize {
		return ErrTooLarge
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var m pkg.Manifest
	if err := dec.Decode(&m); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrTrailingData
	}

	return validate(&m)
}

func validate(m *pkg.Manifest) error {
	for _, f := range []struct{ name, value string }{
		{"name", m.Name},
		{"display_name", m.DisplayName},
		{"description", m.Description},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is required", f.name)
		}
	}

	if len(m.Connectors) == 0 {
		return ErrNoConnectors
	}
	for i := range m.Connectors {
		c := &m.Connectors[i]
		if err := c.Validate(); err != nil {
			return fmt.Errorf("connector #%d: %w", i, err)
		}
		if len(c.Protocols) == 0 {
			return fmt.Errorf("connector #%d: protocols is required", i)
		}
	}
	return nil
}

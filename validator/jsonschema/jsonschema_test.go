package jsonschema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PlakarKorp/go-kloset-sdk/validator/jsonschema"
)

func TestValidate(t *testing.T) {
	for _, dir := range []string{"valid", "invalid"} {
		files, err := filepath.Glob(filepath.Join("testdata", dir, "*.json"))
		require.NoError(t, err)
		require.NotEmpty(t, files, "no gold files in testdata/%s", dir)

		for _, path := range files {
			t.Run(dir+"/"+filepath.Base(path), func(t *testing.T) {
				f, err := os.Open(path)
				require.NoError(t, err)
				t.Cleanup(func() { _ = f.Close() })

				err = jsonschema.ReadAndValidate(f)
				if dir == "valid" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			})
		}
	}
}

func TestValidateOversized(t *testing.T) {
	tests := []struct {
		name string
		size int
		want error
	}{
		{"at limit", jsonschema.MaxSize, nil},
		{"over limit", jsonschema.MaxSize + 1, jsonschema.ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A valid schema padded to exactly tt.size bytes.
			const prefix, suffix = `{"description": "`, `"}`
			doc := prefix + strings.Repeat("a", tt.size-len(prefix)-len(suffix)) + suffix
			require.Len(t, doc, tt.size)

			err := jsonschema.ReadAndValidate(strings.NewReader(doc))
			if tt.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestValidateRefusesFileRef(t *testing.T) {
	target := filepath.Join(t.TempDir(), "target.json")
	require.NoError(t, os.WriteFile(target, []byte(`{"type": "string"}`), 0o600))

	doc := `{"properties": {"location": {"$ref": "file://` + filepath.ToSlash(target) + `"}}}`
	require.Error(t, jsonschema.ReadAndValidate(strings.NewReader(doc)))
}

package manifest_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PlakarKorp/go-kloset-sdk/validator/manifest"
)

// wantPrefix starts the first line of every invalid gold file and names the
// error it must fail with, so a file cannot pass by failing for another reason.
const wantPrefix = "# error: "

func TestValidate(t *testing.T) {
	for _, dir := range []string{"valid", "invalid"} {
		files, err := filepath.Glob(filepath.Join("testdata", dir, "*.yaml"))
		require.NoError(t, err)
		require.NotEmpty(t, files, "no gold files in testdata/%s", dir)

		for _, path := range files {
			t.Run(dir+"/"+filepath.Base(path), func(t *testing.T) {
				f, err := os.Open(path)
				require.NoError(t, err)
				t.Cleanup(func() { _ = f.Close() })

				first, err := bufio.NewReader(f).ReadString('\n')
				require.NoError(t, err)
				_, err = f.Seek(0, 0)
				require.NoError(t, err)

				err = manifest.ReadAndValidate(f)
				if dir == "valid" {
					require.NoError(t, err)
					return
				}
				want, ok := strings.CutPrefix(strings.TrimSpace(first), wantPrefix)
				require.True(t, ok, "first line must start with %q", wantPrefix)
				require.ErrorContains(t, err, want)
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
		{"at limit", manifest.MaxSize, nil},
		{"over limit", manifest.MaxSize + 1, manifest.ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A valid manifest padded to exactly tt.size bytes.
			const prefix = "name: x\ndisplay_name: X\n" +
				"connectors: [{type: importer, executable: x, protocols: [x]}]\n" +
				"description: "
			doc := prefix + strings.Repeat("a", tt.size-len(prefix))
			require.Len(t, doc, tt.size)

			err := manifest.ReadAndValidate(strings.NewReader(doc))
			if tt.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.want)
			}
		})
	}
}

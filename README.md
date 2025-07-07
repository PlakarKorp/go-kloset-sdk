# go-kloset-sdk

The Golang Kloset SDK allows to extend [Plakar][plakar] capabilities
by writing plugins to provide new sources for backups, destinations
for restore and storage for klosets.

<!--
The `go-kloset-sdk`, as the name implies, is for Golang programs.  For
Rust, please take a look at the [rust-kloset-sdk][rust-sdk]
-->

[plakar]: https://github.com/PlakarKorp/plakar
[rust-sdk]: https://github.com/PlakarKorp/rust-kloset-sdk


## Quickstart!

1. Import it in your project:

        $ go get github.com/PlakarKorp/go-kloset-sdk

2. Implement the [Importer][importer], [Exporter][exporter], or
   [Store][storage] interface.

3. Provide one binary per each integration, and call the SDK from your
   `main` function.  For example, the main entrypoint for an Importer
   could look like this:

```go
package main

import (
	"context"

	"github.com/PlakarKorp/go-kloset-sdk"
	"github.com/PlakarKorp/kloset/snapshot/importer"
)

func NewAmazingImporter(ctx context.Context, opts *importer.Options, name string, config map[string]string) (importer.Importer, error) {
	// return your importer here
}

func main() {
	sdk.RunImporter(NewAmazingImporter)
}
```

4. Done!


For a complete example, please take a look at the [fs integration][fs]

[importer]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/snapshot/importer#Importer
[exporter]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/snapshot/exporter#Exporter
[storage]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/storage#Store

[fs]: https://github.com/PlakarKorp/integration-fs

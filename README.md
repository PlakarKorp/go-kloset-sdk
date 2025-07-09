# go-kloset-sdk

[![Go Reference](https://pkg.go.dev/badge/github.com/PlakarKorp/go-kloset-sdk/.svg)](https://pkg.go.dev/github.com/PlakarKorp/go-kloset-sdk/)

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

4. Write a manifest: in order to build a plugin, Plakar needs a
   manifest file which describes the plugin.  A simple manifest for a
   plugin that exposes a source (importer) looks like this:

```yaml
# manifest.yaml
name: awesome-importer
description: an awesome importer
version: 1.2.3
connectors:
- type: importer
  executable: ./awesome
  homepage: https://an.awesome.website
  license: ISC
  protocols: [awesome]
```

5. Create and install the plugin:

```sh
$ plakar pkg create manifest.yaml
# this will create awesome-importer-v1.2.3.ptar
$ plakar pkg install ./awesome-importer-v1.2.3.ptar
$ plakar version
[...]
importers: awesome, [...]
```

6. The plugin can be used by attempting to backup using the `awesome` protocol:

```sh
$ plakar version                # ensure the plugin was loaded
[...]
importers: awesome, [...]
$ plakar backup awesome://place # actually use it
```

7. Done!


For a complete example, please take a look at the [fs integration][fs]

[importer]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/snapshot/importer#Importer
[exporter]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/snapshot/exporter#Exporter
[storage]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/storage#Store

[fs]: https://github.com/PlakarKorp/integration-fs

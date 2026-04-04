# go-kloset-sdk

[![Go Reference](https://pkg.go.dev/badge/github.com/PlakarKorp/go-kloset-sdk/.svg)](https://pkg.go.dev/github.com/PlakarKorp/go-kloset-sdk/)

The Golang Kloset SDK allows to extend [Plakar][plakar] capabilities
by writing plugins to provide new sources for backups, destinations
for restore and storage for klosets.

<!--
The `go-kloset-sdk`, as the name implies, is for Golang programs. For Rust, please take a look at the [rust-kloset-sdk][rust-sdk]
-->

[plakar]: https://github.com/PlakarKorp/plakar
[rust-sdk]: https://github.com/PlakarKorp/rust-kloset-sdk

## Quickstart!

### 1. Import the SDK

```bash
$ go get github.com/PlakarKorp/go-kloset-sdk
```

### 2. Implement the Interface

Implement the [Importer][importer], [Exporter][exporter], or [Store][storage] 
interface depending on your integration type.

### 3. Create Your Main Entry Point

Provide one binary per integration and call the SDK from your `main` function. For example, an Importer entry point looks like this:

```go
package main

import (
	"os"
	sdk "github.com/PlakarKorp/go-kloset-sdk"
	"github.com/yourproject/integration/importer"
)

func main() {
	sdk.EntrypointImporter(os.Args, importer.NewYourImporter)
}
```

Your importer implementation should look like:

```go
package importer

import (
	"context"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
)

type YourImporter struct {
	// your fields
}

func init() {
	importer.Register("yourprotocol", 0, NewYourImporter)
}

func NewYourImporter(ctx context.Context, opts *connectors.Options, name string, config map[string]string) (importer.Importer, error) {
	// initialize and return your importer
}

func (i *YourImporter) Root() string          { /* ... */ }
func (i *YourImporter) Origin() string        { /* ... */ }
func (i *YourImporter) Type() string          { /* ... */ }
func (i *YourImporter) Flags() location.Flags { /* ... */ }
func (i *YourImporter) Ping(ctx context.Context) error { /* ... */ }
func (i *YourImporter) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error { /* ... */ }
func (i *YourImporter) Close(ctx context.Context) error { /* ... */ }
```

### 4. Write a Manifest

Create a manifest file describing your plugin:

```yaml
# manifest.yaml
name: awesome-importer
display_name: Awesome Importer
description: an awesome importer
homepage: https://an.awesome.website
license: ISC
tags: [backup]
api_version: v1
connectors:
  - type: importer
    class: source
    protocols: [awesome]
    executable: ./awesome
```

### 5. Build and Install

```bash
# Build your binary
$ go build -o awesome

# Create the plugin package
$ plakar pkg create manifest.yaml v1.1.0-beta-4
# This creates awesome-importer_v1.1.0-beta.4_linux_amd64.ptar

# Install the plugin
$ plakar pkg add ./awesome-importer_v1.1.0-beta.4_linux_amd64.ptar

# Verify installation
$ plakar pkg show
[...]
awesome-importer@v1.1.0-beta.4
```

### 6. Use Your Plugin

```bash
# Use it to backup
$ plakar at /path/to/repo backup awesome:///path/to/data
```

### 7. Done!
For a complete example, please take a look at the [fs integration][fs]

[importer]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/snapshot/importer#Importer
[exporter]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/snapshot/exporter#Exporter
[storage]: https://pkg.go.dev/github.com/PlakarKorp/kloset@v1.0.1-beta.2/storage#Store

[fs]: https://github.com/PlakarKorp/integration-fs

package sdk

import (
	"errors"
	"fmt"
	"io"
	"os"

	exporter "github.com/PlakarKorp/kloset/snapshot/exporter"
	importer "github.com/PlakarKorp/kloset/snapshot/importer"
	storage "github.com/PlakarKorp/kloset/storage"
)

func checkArgs(args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}
}

func EntrypointImporter(args []string, ctor importer.ImporterFn) {
	checkArgs(args)

	if err := RunImporter(ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "importer plugin process exited with unexpected error: %s", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointExporter(args []string, ctor exporter.ExporterFn) {
	checkArgs(args)

	if err := RunExporter(ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "exporter plugin process exited with unexpected error: %s", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointStorage(args []string, ctor storage.StoreFn) {
	checkArgs(args)

	if err := RunStorage(ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "storage plugin process exited with unexpected error: %s", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

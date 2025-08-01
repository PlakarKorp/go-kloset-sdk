package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/PlakarKorp/go-kloset-sdk"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
)

type FSExporter struct {
	rootDir string
}

func NewFSExporter(ctx context.Context, opts *exporter.Options, name string, config map[string]string) (exporter.Exporter, error) {
	return &FSExporter{
		rootDir: strings.TrimPrefix(config["location"], "fis://"),
	}, nil
}

func (p *FSExporter) Root(ctx context.Context) (string, error) {
	return p.rootDir, nil
}

func (p *FSExporter) CreateDirectory(ctx context.Context, pathname string) error {
	return os.MkdirAll(pathname, 0700)
}

func (p *FSExporter) StoreFile(ctx context.Context, pathname string, fp io.Reader, size int64) error {
	f, err := os.Create(pathname)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, fp); err != nil {
		f.Close()
		return err
	}
	return nil
}

func (p *FSExporter) SetPermissions(ctx context.Context, pathname string, fileinfo *objects.FileInfo) error {
	if err := os.Chmod(pathname, fileinfo.Mode()); err != nil {
		return err
	}
	if os.Getuid() == 0 {
		if err := os.Chown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid())); err != nil {
			return err
		}
	}
	return nil
}

func (p *FSExporter) CreateLink(ctx context.Context, oldname string, newname string, ltype exporter.LinkType) error {
	return errors.ErrUnsupported
}

func (p *FSExporter) Close(ctx context.Context) error {
	return nil
}

func main() {
	if len(os.Args) != 1 {
		fmt.Printf("Usage: %s\n", os.Args[0])
		os.Exit(1)
	}

	if err := sdk.RunExporter(NewFSExporter); err != nil {
		panic(err)
	}
}

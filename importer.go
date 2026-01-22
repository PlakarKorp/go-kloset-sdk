package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	gconn "github.com/PlakarKorp/integration-grpc/v2"
	gimporter "github.com/PlakarKorp/integration-grpc/v2/importer"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// importerPluginServer implements the gRPC server for a Plakar importer plugin.
// It wraps a local importer and exposes its methods over gRPC.
type importerPluginServer struct {
	constructor    importer.ImporterFn
	importer       importer.Importer
	flags          location.Flags
	maxconcurrency int
	readers        *gconn.HoldingReaders

	gimporter.UnimplementedImporterServer
}

// Init initializes the importer with options and configuration.
func (plugin *importerPluginServer) Init(ctx context.Context, req *gimporter.InitRequest) (*gimporter.InitResponse, error) {
	opts := connectors.Options{
		Hostname:        req.Options.Hostname,
		OperatingSystem: req.Options.Os,
		Architecture:    req.Options.Arch,
		CWD:             req.Options.Cwd,
		MaxConcurrency:  int(req.Options.Maxconcurrency),
		Excludes:        req.Options.Excludes,
	}

	imp, err := plugin.constructor(ctx, &opts, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.importer = imp
	plugin.flags = imp.Flags()
	plugin.maxconcurrency = int(req.Options.Maxconcurrency)
	plugin.readers = gconn.NewHoldingReaders()
	return &gimporter.InitResponse{
		Origin: imp.Origin(),
		Type:   imp.Type(),
		Root:   imp.Root(),
		Flags:  uint32(plugin.flags),
	}, nil
}

func (plugin *importerPluginServer) Ping(ctx context.Context, req *gimporter.PingRequest) (*gimporter.PingResponse, error) {
	return nil, plugin.importer.Ping(ctx)
}

func (plugin *importerPluginServer) sendRecords(stream grpc.BidiStreamingServer[gimporter.ImportRequest, gimporter.ImportResponse], records <-chan *connectors.Record) error {
	for record := range records {
		hdr := gimporter.ImportResponse{
			Record: gconn.RecordToProto(record),
		}

		if record.Reader != nil {
			plugin.readers.Track(record)
		}

		if err := stream.Send(&hdr); err != nil {
			return err
		}
	}

	return stream.Send(&gimporter.ImportResponse{Finished: true})
}

func (plugin *importerPluginServer) receiveResults(stream grpc.BidiStreamingServer[gimporter.ImportRequest, gimporter.ImportResponse], results chan<- *connectors.Result) error {
	if results != nil {
		defer close(results)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		result, err := gconn.ResultFromProto(res.Result)
		if err != nil {
			return err
		}

		if results != nil {
			results <- result
		}

		if rd := plugin.readers.Get(&result.Record); rd != nil {
			rd.Close()
		}
	}
}

func (plugin *importerPluginServer) Import(stream grpc.BidiStreamingServer[gimporter.ImportRequest, gimporter.ImportResponse]) error {
	var (
		size    = plugin.maxconcurrency
		records = make(chan *connectors.Record, size)

		wg errgroup.Group
	)

	var results chan *connectors.Result
	if (plugin.flags & location.FLAG_NEEDACK) != 0 {
		results = make(chan *connectors.Result, size)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Go(func() error {
		return plugin.sendRecords(stream, records)
	})

	wg.Go(func() error {
		return plugin.receiveResults(stream, results)
	})

	if err := plugin.importer.Import(ctx, records, results); err != nil {
		return err
	}

	return wg.Wait()
}

func (plugin *importerPluginServer) Open(req *gimporter.OpenRequest, stream grpc.ServerStreamingServer[gimporter.OpenResponse]) error {
	rd := plugin.readers.Get(&connectors.Record{
		Pathname:  req.Record.Pathname,
		IsXattr:   req.Record.IsXattr,
		XattrName: req.Record.XattrName,
		XattrType: objects.Attribute(req.Record.XattrType),
		Target:    req.Record.Target,
	})
	if rd == nil {
		return fs.ErrNotExist
	}
	defer rd.Close()

	var buf = make([]byte, 1024*1024)
	for {
		n, err := rd.Read(buf)
		if n > 0 {
			err := stream.Send(&gimporter.OpenResponse{
				Chunk: buf[:n],
			})
			if err != nil {
				return err
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// Close finalizes the importer and closes all open readers.
func (plugin *importerPluginServer) Close(ctx context.Context, req *gimporter.CloseRequest) (*gimporter.CloseResponse, error) {
	plugin.readers.Close()

	if err := plugin.importer.Close(ctx); err != nil {
		return nil, err
	}
	return &gimporter.CloseResponse{}, nil
}

// RunImporter starts the gRPC server for the importer plugin.
func RunImporter(constructor importer.ImporterFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()
	gimporter.RegisterImporterServer(server, &importerPluginServer{
		constructor: constructor,
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	gconn "github.com/PlakarKorp/integration-grpc"
	gexporter "github.com/PlakarKorp/integration-grpc/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// exporterPluginServer implements the gRPC Exporter service.
// It wraps a Plakar exporter and handles incoming RPCs for exporting snapshot data.
type exporterPluginServer struct {
	// constructor is the factory function used to create a new exporter instance.
	constructor exporter.ExporterFn

	// exporter is the underlying Plakar exporter implementation.
	exporter exporter.Exporter

	maxConcurrency int
	flags          location.Flags

	// UnimplementedExporterServer must be embedded for forward compatibility.
	gexporter.UnimplementedExporterServer
}

func unwrap(err error) error {
	if err == nil {
		return nil
	}

	status, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Canceled:
		return context.Canceled
	default:
		return fmt.Errorf("%s", status.Message())
	}
}

// Init initializes the exporter with given options and configuration.
//
// It must be called first. It uses the constructor to create the concrete exporter.
func (plugin *exporterPluginServer) Init(ctx context.Context, req *gexporter.InitRequest) (*gexporter.InitResponse, error) {
	opts := connectors.Options{
		MaxConcurrency:  int(req.Options.Maxconcurrency),
		Hostname:        req.Options.Hostname,
		OperatingSystem: req.Options.Os,
		Architecture:    req.Options.Arch,
		CWD:             req.Options.Cwd,
	}

	exp, err := plugin.constructor(ctx, &opts, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.exporter = exp
	plugin.maxConcurrency = int(req.Options.Maxconcurrency)
	plugin.flags = exp.Flags()

	return &gexporter.InitResponse{
		Origin: exp.Origin(),
		Type:   exp.Type(),
		Root:   exp.Root(),
		Flags:  uint32(plugin.flags),
	}, nil
}

// Close finalizes the exporter, ensuring that all data is flushed and resources are released.
func (plugin *exporterPluginServer) Ping(ctx context.Context, req *gexporter.PingRequest) (*gexporter.PingResponse, error) {
	err := plugin.exporter.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return &gexporter.PingResponse{}, nil
}

type streamReader struct {
	end    chan struct{}
	eof    bool
	stream grpc.BidiStreamingServer[gexporter.ExportRequest, gexporter.ExportResponse]
	buf    bytes.Buffer
}

func open(stream grpc.BidiStreamingServer[gexporter.ExportRequest, gexporter.ExportResponse], end chan struct{}) io.ReadCloser {
	return &streamReader{
		stream: stream,
		end:    end,
	}
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	if s.buf.Len() != 0 {
		n, err = s.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}

	fileResponse, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, fmt.Errorf("failed to receive file data: %w", unwrap(err))
	}

	if chunk := fileResponse.GetChunk(); chunk != nil {
		if len(chunk) == 0 {
			s.eof = true
		}

		s.buf.Write(chunk)
		n, err = s.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, fmt.Errorf("unexpected response: %v", fileResponse)
}

// We still drain this in order to avoid a misuse of the API (where someone
// would request less than what the server is sending), which leads to hangs.
func (s *streamReader) Close() error {
	defer close(s.end)

	for !s.eof {
		res, err := s.stream.Recv()
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		} else if err != nil {
			return err
		}

		if chunk := res.GetChunk(); chunk != nil {
			if len(chunk) == 0 {
				break
			}
		} else {
			return fmt.Errorf("unexpected response: %v", res)
		}
	}
	return nil
}

func (plugin *exporterPluginServer) receiveRecords(stream grpc.BidiStreamingServer[gexporter.ExportRequest, gexporter.ExportResponse], records chan<- *connectors.Record) error {
	defer close(records)

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		r := res.GetRecord()
		if r == nil {
			return fmt.Errorf("bad type; expected a record, got %t", res.Packet)
		}

		record, err := gconn.RecordFromProto(r)
		if err != nil {
			return err
		}

		var (
			rd  io.ReadCloser
			end chan struct{}
		)

		if r.HasReader {
			end = make(chan struct{})
			rd = open(stream, end)

			record.Reader = rd
		}

		records <- record
		if end != nil {
			<-end
		}
	}
}

func (plugin *exporterPluginServer) transmitResults(stream grpc.BidiStreamingServer[gexporter.ExportRequest, gexporter.ExportResponse], results <-chan *connectors.Result) error {
	for result := range results {
		hdr := gexporter.ExportResponse{
			Result: gconn.ResultToProto(result),
		}
		if err := stream.Send(&hdr); err != nil {
			return err
		}
	}

	return nil
}

func (plugin *exporterPluginServer) Export(stream grpc.BidiStreamingServer[gexporter.ExportRequest, gexporter.ExportResponse]) error {
	var (
		size    = plugin.maxConcurrency
		records = make(chan *connectors.Record, size)
		results = make(chan *connectors.Result, size)
		errch   = make(chan error, 1)
		wg      = errgroup.Group{}
	)

	ctx, cancel := context.WithCancel(context.Background())

	wg.Go(func() error {
		return plugin.receiveRecords(stream, records)
	})

	wg.Go(func() error {
		defer cancel()
		return plugin.transmitResults(stream, results)
	})

	go func() {
		if err := wg.Wait(); err != nil {
			errch <- err
		}
		close(errch)
	}()

	if err := plugin.exporter.Export(ctx, records, results); err != nil {
		return err
	}
	return <-errch
}

// Close finalizes the exporter, ensuring that all data is flushed and resources are released.
func (plugin *exporterPluginServer) Close(ctx context.Context, req *gexporter.CloseRequest) (*gexporter.CloseResponse, error) {
	err := plugin.exporter.Close(ctx)
	if err != nil {
		return nil, err
	}

	return &gexporter.CloseResponse{}, nil
}

// RunExporter launches the gRPC server for an exporter plugin.
//
// The given constructor will be used to initialize the exporter instance.
func RunExporter(constructor exporter.ExporterFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()

	gexporter.RegisterExporterServer(server, &exporterPluginServer{
		constructor: constructor,
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

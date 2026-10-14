package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	gconn "github.com/PlakarKorp/integration-grpc"
	gexporter "github.com/PlakarKorp/integration-grpc/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type exporterPluginServer struct {
	constructor    exporter.ExporterFn
	exporter       exporter.Exporter
	maxConcurrency int
	flags          location.Flags

	// inflight matches the records on the Export stream with the
	// PutData streams carrying their payloads.
	inflight payloads

	gexporter.UnimplementedExporterServer
}

var (
	ErrExporterClosed = errors.New("the exporter is closed")
)

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
		Origin:     exp.Origin(),
		Type:       exp.Type(),
		Root:       exp.Root(),
		Flags:      uint32(plugin.flags),
		HasPutData: true,
	}, nil
}

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
		return 0, fmt.Errorf("failed to receive file data: %w", err)
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

		var (
			record *connectors.Record
			end    chan struct{}
		)

		switch p := res.Packet.(type) {
		case *gexporter.ExportRequest_Record:
			record, end, err = plugin.receiveRecord(stream, p.Record)
		case *gexporter.ExportRequest_Entry:
			record, err = plugin.receiveEntry(p.Entry)
		default:
			err = fmt.Errorf("bad type; expected a record, got %t", res.Packet)
		}
		if err != nil {
			return err
		}

		records <- record

		// the legacy protocol carries the payload on this very
		// stream, so we cannot read the next record before the
		// exporter is done with this one.
		if end != nil {
			<-end
		}
	}
}

// receiveRecord handles the legacy protocol, where the payload follows
// the record on the Export stream.
func (plugin *exporterPluginServer) receiveRecord(stream grpc.BidiStreamingServer[gexporter.ExportRequest, gexporter.ExportResponse], r *gconn.Record) (*connectors.Record, chan struct{}, error) {
	record, err := gconn.RecordFromProto(r)
	if err != nil {
		return nil, nil, err
	}

	if !r.HasReader {
		return record, nil, nil
	}

	end := make(chan struct{})
	record.Reader = open(stream, end)

	return record, end, nil
}

// receiveEntry handles the entry protocol, where the payload is either
// inline or on a PutData stream of its own.
func (plugin *exporterPluginServer) receiveEntry(entry *gexporter.ExportEntry) (*connectors.Record, error) {
	if entry.Record == nil {
		return nil, fmt.Errorf("entry without a record")
	}

	record, err := gconn.RecordFromProto(entry.Record)
	if err != nil {
		return nil, err
	}

	switch {
	case entry.Chunk != nil && entry.Id != 0:
		return nil, fmt.Errorf("%s: payload both inline and on a stream", record.Pathname)
	case entry.Chunk != nil:
		record.Reader = io.NopCloser(bytes.NewReader(entry.Chunk))
	case entry.Id != 0:
		rd, err := plugin.inflight.reader(entry.Id)
		if err != nil {
			return nil, err
		}
		record.Reader = rd
	case entry.Record.HasReader:
		return nil, fmt.Errorf("%s: payload announced but not sent", record.Pathname)
	}

	return record, nil
}

// PutData receives the payload of one record, which the Export stream
// named with the same id.  Either stream can get here first.
func (plugin *exporterPluginServer) PutData(stream grpc.ClientStreamingServer[gexporter.PutDataRequest, gexporter.PutDataResponse]) error {
	var wr *io.PipeWriter

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if wr != nil {
				wr.CloseWithError(err)
			}
			return err
		}

		switch p := req.Packet.(type) {
		case *gexporter.PutDataRequest_Id:
			if wr != nil {
				return fmt.Errorf("payload %d: id sent twice", p.Id)
			}
			if wr, err = plugin.inflight.writer(p.Id); err != nil {
				return err
			}

		case *gexporter.PutDataRequest_Chunk:
			if wr == nil {
				return fmt.Errorf("payload data before its id")
			}
			if _, err := wr.Write(p.Chunk); err != nil {
				// the exporter closed the record early;
				// say so rather than let the client take
				// it for a broken stream.
				if errors.Is(err, io.ErrClosedPipe) {
					return stream.SendAndClose(&gexporter.PutDataResponse{Aborted: true})
				}
				return err
			}

		case *gexporter.PutDataRequest_Error:
			if wr == nil {
				return fmt.Errorf("payload error before its id")
			}
			// the client could not read this record; the
			// exporter decides what that means for it.
			wr.CloseWithError(errors.New(p.Error))

		default:
			return fmt.Errorf("bad type; expected a payload, got %t", req.Packet)
		}
	}

	if wr == nil {
		return fmt.Errorf("payload without an id")
	}
	wr.Close()

	return stream.SendAndClose(&gexporter.PutDataResponse{})
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
		wg      = errgroup.Group{}
	)

	ctx, cancel := context.WithCancel(stream.Context())

	wg.Go(func() error {
		return plugin.receiveRecords(stream, records)
	})

	wg.Go(func() error {
		defer cancel()
		return plugin.transmitResults(stream, results)
	})

	if err := plugin.exporter.Export(ctx, records, results); err != nil {
		return err
	}

	return wg.Wait()
}

func (plugin *exporterPluginServer) Close(ctx context.Context, req *gexporter.CloseRequest) (*gexporter.CloseResponse, error) {
	// we are done for good: release the payloads still waiting,
	// before the exporter gets a chance to wait on one of them.
	plugin.inflight.fail(ErrExporterClosed)

	err := plugin.exporter.Close(ctx)
	if err != nil {
		return nil, err
	}

	return &gexporter.CloseResponse{}, nil
}

// RunExporter launches the gRPC server for an exporter plugin.
//
// The given constructor will be used to initialize the exporter instance.
func RunExporter(ctx context.Context, constructor exporter.ExporterFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunExporterOn(ctx, constructor, listener)
}

func RunExporterOn(ctx context.Context, constructor exporter.ExporterFn, listener net.Listener) error {
	server := grpc.NewServer()

	plugin := &exporterPluginServer{constructor: constructor}
	gexporter.RegisterExporterServer(server, plugin)

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			// an exporter parked on a payload that is never
			// going to arrive now would never get to wind
			// down: Stop() does not wait for the handlers.
			plugin.inflight.fail(fmt.Errorf("the plugin is shutting down: %w", ctx.Err()))
			server.Stop()
		case <-done:
		}
	}()

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

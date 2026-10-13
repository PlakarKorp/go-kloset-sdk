package sdk

import (
	"bytes"
	"context"
	"io"
	"iter"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	gconn "github.com/PlakarKorp/integration-grpc"
	gexporter "github.com/PlakarKorp/integration-grpc/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// A fake exporter, standing in for the plugin's own implementation
// ---------------------------------------------------------------------------

type gotRecord struct {
	pathname string
	payload  []byte
	err      error
}

type fakeExporter struct {
	mu  sync.Mutex
	got []gotRecord

	// readLimit, when positive, makes the exporter close a payload
	// after that many bytes instead of reading it to the end.
	readLimit int
}

func (f *fakeExporter) Origin() string              { return "o" }
func (f *fakeExporter) Type() string                { return "t" }
func (f *fakeExporter) Root() string                { return "/" }
func (f *fakeExporter) Flags() location.Flags       { return 0 }
func (f *fakeExporter) Ping(context.Context) error  { return nil }
func (f *fakeExporter) Close(context.Context) error { return nil }

func (f *fakeExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)

	var wg sync.WaitGroup
	for record := range records {
		wg.Go(func() {
			results <- f.consume(record)
		})
	}
	wg.Wait()

	return nil
}

func (f *fakeExporter) consume(record *connectors.Record) *connectors.Result {
	got := gotRecord{pathname: record.Pathname}

	if record.Reader != nil {
		var rd io.Reader = record.Reader
		if f.readLimit > 0 {
			rd = io.LimitReader(rd, int64(f.readLimit))
		}
		got.payload, got.err = io.ReadAll(rd)
	}

	f.mu.Lock()
	f.got = append(f.got, got)
	f.mu.Unlock()

	return record.Ok()
}

func (f *fakeExporter) records() []gotRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]gotRecord(nil), f.got...)
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

func dialPlugin(t *testing.T, exp exporter.Exporter) gexporter.ExporterClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	gexporter.RegisterExporterServer(srv, &exporterPluginServer{
		constructor: func(context.Context, *connectors.Options, string, map[string]string) (exporter.Exporter, error) {
			return exp, nil
		},
	})
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	client := gexporter.NewExporterClient(conn)
	if _, err := client.Init(t.Context(), &gexporter.InitRequest{
		Options: &gexporter.Options{Maxconcurrency: 4},
		Proto:   "p",
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	return client
}

func recordOf(pathname string, size int64) *gconn.Record {
	return gconn.RecordToProto(&connectors.Record{
		Pathname: pathname,
		FileInfo: objects.FileInfo{Lname: pathname, Lsize: size, Lmode: 0o644},
		// RecordToProto only looks at whether this is set
		Reader: io.NopCloser(strings.NewReader("")),
	})
}

// chunked yields body one message at a time, the way a client sends it.
func chunked(body []byte, size int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for len(body) > size {
			if !yield(body[:size]) {
				return
			}
			body = body[size:]
		}
		yield(body)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// The old protocol, where the payload follows the record on the Export
// stream itself, has to keep working untouched.
func TestExporterServerLegacyProtocol(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 32*1024)

	exp := &fakeExporter{}
	client := dialPlugin(t, exp)

	stream, err := client.Export(t.Context())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()

	send := func(packet *gexporter.ExportRequest) {
		t.Helper()
		if err := stream.Send(packet); err != nil {
			t.Errorf("send: %v", err)
		}
	}

	for _, pathname := range []string{"/a", "/b"} {
		send(&gexporter.ExportRequest{
			Packet: &gexporter.ExportRequest_Record{
				Record: recordOf(pathname, int64(len(body))),
			},
		})
		for chunk := range chunked(body, 4096) {
			pkt := &gexporter.ExportRequest_Chunk{Chunk: chunk}
			send(&gexporter.ExportRequest{Packet: pkt})
		}
		// the terminator telling the reader it reached the end
		send(&gexporter.ExportRequest{
			Packet: &gexporter.ExportRequest_Chunk{Chunk: nil},
		})
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the export to finish")
	}

	got := exp.records()
	if len(got) != 2 {
		t.Fatalf("exporter saw %d records, want 2", len(got))
	}
	for i, rec := range got {
		if rec.err != nil {
			t.Errorf("record %d: %v", i, rec.err)
		}
		if !bytes.Equal(rec.payload, body) {
			t.Errorf("record %d: payload is %d bytes, want %d", i, len(rec.payload), len(body))
		}
	}
}

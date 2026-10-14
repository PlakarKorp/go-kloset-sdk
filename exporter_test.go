package sdk

import (
	"bytes"
	"context"
	"errors"
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

	// reading and seen, when set, report a record about to be read
	// and one that has been.
	reading chan struct{}
	seen    chan gotRecord
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

	if f.reading != nil {
		f.reading <- struct{}{}
	}

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

	if f.seen != nil {
		f.seen <- got
	}

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
	t.Cleanup(srv.Stop)

	return dialOn(t, lis)
}

// dialOn connects to a plugin already listening on lis and inits it.
func dialOn(t *testing.T, lis *bufconn.Listener) gexporter.ExporterClient {
	t.Helper()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

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

func entryOf(pathname string, size int64) *gexporter.ExportEntry {
	return &gexporter.ExportEntry{Record: recordOf(pathname, size)}
}

// putData uploads one payload the way the client does.
func putData(t *testing.T, client gexporter.ExporterClient, id uint64, body []byte, readErr error) *gexporter.PutDataResponse {
	t.Helper()

	stream, err := client.PutData(t.Context())
	if err != nil {
		t.Fatalf("PutData: %v", err)
	}

	// the exporter may hang up at any point, which ends the stream
	// with io.EOF and is answered by the response, not by the send.
	send := func(req *gexporter.PutDataRequest) bool {
		err := stream.Send(req)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("send: %v", err)
		}
		return err == nil
	}

	send(&gexporter.PutDataRequest{
		Packet: &gexporter.PutDataRequest_Id{Id: id},
	})

	if readErr != nil {
		send(&gexporter.PutDataRequest{
			Packet: &gexporter.PutDataRequest_Error{Error: readErr.Error()},
		})
	} else {
		for chunk := range chunked(body, 4096) {
			pkt := &gexporter.PutDataRequest_Chunk{Chunk: chunk}
			if !send(&gexporter.PutDataRequest{Packet: pkt}) {
				break
			}
		}
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	return res
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

// exportEntries runs a whole Export session, sending entries and
// uploading the bodies the dispatch function hands back.
func exportEntries(t *testing.T, client gexporter.ExporterClient, entries []*gexporter.ExportEntry, upload func()) {
	t.Helper()

	stream, err := client.Export(t.Context())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	results := make(chan int, 1)
	go func() {
		n := 0
		for {
			if _, err := stream.Recv(); err != nil {
				results <- n
				return
			}
			n++
		}
	}()

	for _, entry := range entries {
		if err := stream.Send(&gexporter.ExportRequest{
			Packet: &gexporter.ExportRequest_Entry{Entry: entry},
		}); err != nil {
			t.Fatalf("send entry: %v", err)
		}
	}

	if upload != nil {
		upload()
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	select {
	case <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the export to finish")
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestExporterServerInlinePayload(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("data")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := &fakeExporter{}
			client := dialPlugin(t, exp)

			entry := entryOf("/f", int64(len(tc.body)))
			entry.Chunk = tc.body

			exportEntries(t, client, []*gexporter.ExportEntry{entry}, nil)

			got := exp.records()
			if len(got) != 1 {
				t.Fatalf("exporter saw %d records, want 1", len(got))
			}
			if !bytes.Equal(got[0].payload, tc.body) {
				t.Errorf("payload %q, want %q", got[0].payload, tc.body)
			}
		})
	}
}

// The record and its body travel on different streams, and gRPC does
// not order them: both arrival orders have to work.
func TestExporterServerStreamedPayload(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 128*1024)

	for _, tc := range []struct {
		name        string
		uploadFirst bool
	}{
		{"entry first", false},
		{"payload first", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := &fakeExporter{}
			client := dialPlugin(t, exp)

			entry := entryOf("/f", int64(len(body)))
			entry.Id = 1

			upload := func() { putData(t, client, 1, body, nil) }
			if tc.uploadFirst {
				done := make(chan struct{})
				go func() {
					defer close(done)
					putData(t, client, 1, body, nil)
				}()
				// the upload blocks until the record shows up
				exportEntries(t, client, []*gexporter.ExportEntry{entry}, nil)
				<-done
			} else {
				exportEntries(t, client, []*gexporter.ExportEntry{entry}, upload)
			}

			got := exp.records()
			if len(got) != 1 {
				t.Fatalf("exporter saw %d records, want 1", len(got))
			}
			if got[0].err != nil {
				t.Fatalf("read: %v", got[0].err)
			}
			if !bytes.Equal(got[0].payload, body) {
				t.Errorf("payload: got %d bytes, want %d", len(got[0].payload), len(body))
			}
		})
	}
}

// A payload the client could not read reaches the exporter as a read
// error on that record, and that record only.
func TestExporterServerPayloadError(t *testing.T) {
	exp := &fakeExporter{}
	client := dialPlugin(t, exp)

	entry := entryOf("/f", 4096)
	entry.Id = 1

	exportEntries(t, client, []*gexporter.ExportEntry{entry}, func() {
		putData(t, client, 1, nil, errors.New("boom"))
	})

	got := exp.records()
	if len(got) != 1 {
		t.Fatalf("exporter saw %d records, want 1", len(got))
	}
	if got[0].err == nil || !strings.Contains(got[0].err.Error(), "boom") {
		t.Errorf("read error %v, want it to mention boom", got[0].err)
	}
}

// An exporter that stops reading a record early says so through the
// response, so that the client can tell it from a broken stream.
func TestExporterServerAbortedPayload(t *testing.T) {
	exp := &fakeExporter{readLimit: 16}
	client := dialPlugin(t, exp)

	entry := entryOf("/f", 1<<20)
	entry.Id = 1

	var res *gexporter.PutDataResponse
	exportEntries(t, client, []*gexporter.ExportEntry{entry}, func() {
		res = putData(t, client, 1, bytes.Repeat([]byte("x"), 1<<20), nil)
	})

	if res == nil || !res.Aborted {
		t.Errorf("PutData response %v, want aborted", res)
	}
}

// The whole point: the receive goroutine no longer waits for one
// payload before taking the next record.
func TestExporterServerRecordsDoNotWaitOnPayloads(t *testing.T) {
	const count = 4

	exp := &fakeExporter{}
	client := dialPlugin(t, exp)

	body := bytes.Repeat([]byte("x"), 64*1024)

	entries := make([]*gexporter.ExportEntry, count)
	for i := range entries {
		entries[i] = entryOf("/f", int64(len(body)))
		entries[i].Id = uint64(i + 1)
	}

	// Every entry is sent before any payload is uploaded, which the
	// legacy protocol could not do: it would deadlock on the first.
	exportEntries(t, client, entries, func() {
		var wg sync.WaitGroup
		for i := range entries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				putData(t, client, uint64(i+1), body, nil)
			}()
		}
		wg.Wait()
	})

	got := exp.records()
	if len(got) != count {
		t.Fatalf("exporter saw %d records, want %d", len(got), count)
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

// the exporter should terminate on Close even if it is holding a
// payload that was never picked up.
func TestExporterServerReleasesPayloadsOnClose(t *testing.T) {
	exp := &fakeExporter{
		reading: make(chan struct{}, 1),
		seen:    make(chan gotRecord, 1),
	}
	client := dialPlugin(t, exp)

	stream, err := client.Export(t.Context())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	entry := entryOf("/f", 1<<20)
	entry.Id = 1
	if err := stream.Send(&gexporter.ExportRequest{
		Packet: &gexporter.ExportRequest_Entry{Entry: entry},
	}); err != nil {
		t.Fatalf("send entry: %v", err)
	}

	// Wait until the exporter holds the record, so that we know it
	// is about to block on a payload we are never going to upload.
	select {
	case <-exp.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the exporter to get the record")
	}

	if _, err := client.Close(t.Context(), &gexporter.CloseRequest{}); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case got := <-exp.seen:
		if got.err == nil {
			t.Error("the payload read succeeded, want the session error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the exporter is still blocked on a payload nobody will send")
	}
}

// a shutdown has to release an exporter parked on a payload, so that
// it gets to wind down instead of staying blocked.
func TestRunExporterOnReleasesPayloadsOnShutdown(t *testing.T) {
	exp := &fakeExporter{
		reading: make(chan struct{}, 1),
		seen:    make(chan gotRecord, 1),
	}

	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(t.Context())

	served := make(chan error, 1)
	go func() {
		served <- RunExporterOn(ctx, func(context.Context, *connectors.Options, string, map[string]string) (exporter.Exporter, error) {
			return exp, nil
		}, lis)
	}()

	client := dialOn(t, lis)

	stream, err := client.Export(t.Context())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	entry := entryOf("/f", 1<<20)
	entry.Id = 1
	if err := stream.Send(&gexporter.ExportRequest{
		Packet: &gexporter.ExportRequest_Entry{Entry: entry},
	}); err != nil {
		t.Fatalf("send entry: %v", err)
	}

	select {
	case <-exp.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the exporter to get the record")
	}

	cancel()

	select {
	case got := <-exp.seen:
		if got.err == nil {
			t.Error("the payload read succeeded, want the shutdown error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the exporter is still blocked on a payload nobody will send")
	}

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("RunExporterOn: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunExporterOn did not return")
	}
}

// the handshake is what puts a client on the entry protocol.
func TestExporterServerAnnouncesPutData(t *testing.T) {
	plugin := &exporterPluginServer{
		constructor: func(context.Context, *connectors.Options, string, map[string]string) (exporter.Exporter, error) {
			return &fakeExporter{}, nil
		},
	}

	res, err := plugin.Init(t.Context(), &gexporter.InitRequest{
		Options: &gexporter.Options{Maxconcurrency: 4},
		Proto:   "p",
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if !res.HasPutData {
		t.Error("the handshake does not announce PutData support")
	}
}

package sdk

import (
    "context"
    "fmt"
    "net"
    "time"

    "github.com/PlakarKorp/go-kloset-sdk/pkg/importer"
    "google.golang.org/grpc"
    "google.golang.org/protobuf/types/known/timestamppb"
)

type InfoRequest struct {
}
// type InfoRequest = importer.InfoRequest

type InfoResponse struct {
    Type   string
    Origin string
    Root   string
}
// type InfoResponse = importer.InfoResponse

type ScanRequest struct {
}
// type ScanRequest = importer.ScanRequest

type ScanResponseStreamer struct {
    impl importer.Importer_ScanServer
}

func (s *ScanResponseStreamer) Context() context.Context {
    return s.impl.Context()
}

func (s *ScanResponseStreamer) Send(resp *importer.ScanResponse) error {
    return s.impl.Send(resp)
}
// type ScanResponseStreamer = importer.Importer_ScanServer

type ScanResponse struct {
	Record *ScanResponseRecord
	Error  *ScanResponseError
}
// type ScanResponse = importer.ScanResponse

type ScanResponseError struct {
	Error *ScanError
}
// type ScanResponseError = importer.ScanResponse_Error

type ScanError struct {
	Code    int32
	Message string
}
// type ScanError = importer.ScanError

type ScanResponseRecord struct {
	Record *ScanRecord
}
// type ScanResponseRecord = importer.ScanResponse_Record

type ScanRecord struct {
	Path     string
	FileInfo *ScanRecordFileInfo
}
// type ScanRecord = importer.ScanRecord

type ScanRecordFileInfo struct {
	Size      int64
	CreatedAt ImporterTimestamp
	UpdatedAt ImporterTimestamp
}
// type ScanRecordFileInfo = importer.ScanRecordFileInfo

type ReadRequest struct {
	Path string
}
// type ReadRequest = importer.ReadRequest

type ReadResponseStramer struct {
	impl importer.Importer_ReadServer
}

func (s *ReadResponseStramer) Context() context.Context {
	return s.impl.Context()
}

func (s *ReadResponseStramer) Send(resp *importer.ReadResponse) error {
	return s.impl.Send(resp)
}
// type ReadResponseStramer = importer.Importer_ReadServer

type ReadResponse struct {
	Data []byte
	Done bool
}
// type ReadResponse = importer.ReadResponse

type ImporterPlugin interface {
    Info(ctx context.Context, req *InfoRequest) (*InfoResponse, error)
    Scan(req *ScanRequest, stream ScanResponseStreamer) error
    Read(req *ReadRequest, stream ReadResponseStramer) error
}

type ImporterPluginServer struct {
    imp ImporterPlugin

    importer.UnimplementedImporterServer
}

func (plugin *ImporterPluginServer) Info(ctx context.Context, req *importer.InfoRequest) (*importer.InfoResponse, error) {
    resp, err := plugin.imp.Info(ctx, &InfoRequest{})

    if err != nil {
        return nil, err
    }

    return &importer.InfoResponse{
        Type:   resp.Type,
        Origin: resp.Origin,
        Root:   resp.Root,
    }, nil
}

func (plugin *ImporterPluginServer) Scan(req *importer.ScanRequest, stream importer.Importer_ScanServer) error {
    err := plugin.imp.Scan(&ScanRequest{}, ScanResponseStreamer{impl: stream})
    return err
}

func (plugin *ImporterPluginServer) Read(req *importer.ReadRequest, stream importer.Importer_ReadServer) error {
    return plugin.imp.Read(&ReadRequest{Path: req.Pathname}, ReadResponseStramer{impl: stream})
}

func RunImporter(imp ImporterPlugin) error {
    listenAddr, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 50052))
    if err != nil {
        return err
    }

    server := grpc.NewServer()
    fmt.Printf("server listening on %s\n", listenAddr.Addr())

    importer.RegisterImporterServer(server, &ImporterPluginServer{imp: imp})

    if err := server.Serve(listenAddr); err != nil {
        return err
    }
    return nil
}

type ImporterTimestamp = *timestamppb.Timestamp

func NewTimestamp(time time.Time) ImporterTimestamp {
    return timestamppb.New(time)
}

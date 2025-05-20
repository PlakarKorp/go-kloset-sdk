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

// type InfoRequest = importer.InfoRequest
type InfoRequest struct {
	impl *importer.InfoRequest
}
type InfoResponse = importer.InfoResponse

type ScanRequest = importer.ScanRequest
type ScanResponseStreamer = importer.Importer_ScanServer

type ScanResponse = importer.ScanResponse
type ScanResponseError = importer.ScanResponse_Error
type ScanError = importer.ScanError

type ScanResponseRecord = importer.ScanResponse_Record
type ScanRecord = importer.ScanRecord
type ScanRecordFileInfo = importer.ScanRecordFileInfo

type ReadRequest = importer.ReadRequest
type ReadResponseStramer = importer.Importer_ReadServer

type ReadResponse = importer.ReadResponse

type ImporterPlugin interface {
	Info(ctx context.Context, req *InfoRequest) (*InfoResponse, error)
	Scan(req *ScanRequest, stream ScanResponseStreamer) error
	Read(req *ReadRequest, stream ReadResponseStramer) error
}

type ImporterPluginServer struct {
	imp ImporterPlugin

	importer.UnimplementedImporterServer
}

func (plugin *ImporterPluginServer) Info(ctx context.Context, req *InfoRequest) (*importer.InfoResponse, error) {
	return plugin.imp.Info(ctx, req.impl)
}

func (plugin *ImporterPluginServer) Scan(req *importer.ScanRequest, stream importer.Importer_ScanServer) error {
	return plugin.imp.Scan(req, stream)
}

func (plugin *ImporterPluginServer) Read(req *importer.ReadRequest, stream importer.Importer_ReadServer) error {
	return plugin.imp.Read(req, stream)
}

func RunImporter(imp ImporterPlugin) error {
	listenAddr, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 50051))
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

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

func (s *ScanResponseStreamer) Send(resp *ScanResponse) error {
	importerResp := &importer.ScanResponse{
		Pathname: resp.Pathname,
	}
	
	// Convert the Result field based on its type
	if record := resp.GetRecord(); record != nil {
		importerRecord := &importer.ScanRecord{
			Target:         record.Record.Target,
			FileAttributes: record.Record.FileAttributes,
		}
		if record.Record.Fileinfo != nil {
			importerRecord.Fileinfo = &importer.ScanRecordFileInfo{
				Name:      record.Record.Fileinfo.Name,
				Size:      record.Record.Fileinfo.Size,
				Mode:      record.Record.Fileinfo.Mode,
				ModTime:   record.Record.Fileinfo.ModTime,
				Dev:       record.Record.Fileinfo.Dev,
				Ino:       record.Record.Fileinfo.Ino,
				Uid:       record.Record.Fileinfo.Uid,
				Gid:       record.Record.Fileinfo.Gid,
				Nlink:     record.Record.Fileinfo.Nlink,
				Username:  record.Record.Fileinfo.Username,
				Groupname: record.Record.Fileinfo.Groupname,
				Flags:     record.Record.Fileinfo.Flags,
			}
		}
		if record.Record.Xattr != nil {
			importerRecord.Xattr = &importer.ExtendedAttribute{
				Name: record.Record.Xattr.Name,
				Type: importer.ExtendedAttributeType(record.Record.Xattr.Type),
			}
		}
		importerResp.Result = &importer.ScanResponse_Record{
			Record: importerRecord,
		}
	} else if err := resp.GetError(); err != nil {
		importerResp.Result = &importer.ScanResponse_Error{
			Error: &importer.ScanError{
				Message: err.Error.Message,
			},
		}
	}
	return s.impl.Send(importerResp)
}

// type ScanResponseStreamer = importer.Importer_ScanServer

// isScanResponse_Result is an interface to enforce the oneof constraint
type isScanResponse_Result interface {
	isScanResponse_Result()
}

// Implement the interface for the possible oneof types
func (*ScanResponseRecord) isScanResponse_Result() {}
func (*ScanResponseError) isScanResponse_Result() {}

type ScanResponse struct {
	Pathname string

	Result isScanResponse_Result
}

// Convenience getters for the oneof field
func (m *ScanResponse) GetRecord() *ScanResponseRecord {
	if x, ok := m.Result.(*ScanResponseRecord); ok {
		return x
	}
	return nil
}

func (m *ScanResponse) GetError() *ScanResponseError {
	if x, ok := m.Result.(*ScanResponseError); ok {
		return x
	}
	return nil
}

// type ScanResponse = importer.ScanResponse

type ScanResponseError struct {
	Error *ScanError
}

// type ScanResponseError = importer.ScanResponse_Error

type ScanError struct {
	Message string
}

// type ScanError = importer.ScanError

type ScanResponseRecord struct {
	Record *ScanRecord
}

// type ScanResponseRecord = importer.ScanResponse_Record

type ScanRecord struct {
	Target         string
	Fileinfo       *ScanRecordFileInfo
	FileAttributes uint32
	Xattr          *ExtendedAttribute
}

// type ScanRecord = importer.ScanRecord

type ExtendedAttribute struct {
	Name string
	Type ExtendedAttributeType
}

type ExtendedAttributeType int32

const (
	ExtendedAttributeType_EXTENDED_ATTRIBUTE_TYPE_EXTENDED    ExtendedAttributeType = 0
	ExtendedAttributeType_EXTENDED_ATTRIBUTE_TYPE_ADS         ExtendedAttributeType = 1
	ExtendedAttributeType_EXTENDED_ATTRIBUTE_TYPE_UNSPECIFIED ExtendedAttributeType = 2
)

type ScanRecordFileInfo struct {
	Name      string
	Size      int64
	Mode      uint32
	ModTime   *timestamppb.Timestamp
	Dev       uint64
	Ino       uint64
	Uid       uint64
	Gid       uint64
	Nlink     uint32
	Username  string
	Groupname string
	Flags     uint32
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

func (s *ReadResponseStramer) Send(resp *ReadResponse) error {
	importerResp := &importer.ReadResponse{
		Data: resp.Data,
	}
	return s.impl.Send(importerResp)
}

// type ReadResponseStramer = importer.Importer_ReadServer

type ReadResponse struct {
	Data []byte
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

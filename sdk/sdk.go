package sdk

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/PlakarKorp/go-kloset-sdk/pkg/importer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	impor "github.com/PlakarKorp/plakar/snapshot/importer"
)

type ImporterPluginServer struct {
	imp impor.Importer

	importer.UnimplementedImporterServer
}

func (plugin *ImporterPluginServer) Info(ctx context.Context, req *importer.InfoRequest) (*importer.InfoResponse, error) {
	return &importer.InfoResponse{
		Type:   plugin.imp.Type(),
		Origin: plugin.imp.Origin(),
		Root:   plugin.imp.Root(),
	}, nil
}

func (plugin *ImporterPluginServer) Scan(req *importer.ScanRequest, stream importer.Importer_ScanServer) error {
	scanResult, err := plugin.imp.Scan()
	if err != nil {
		return err
	}
	for result := range scanResult {
		switch {
		case result.Record != nil:
			if err := stream.Context().Err(); err != nil {
				fmt.Printf("Client connection closed: %v\n", err)
				return err
			}

			var xattr *importer.ExtendedAttribute
			if result.Record.IsXattr {
				xattr = &importer.ExtendedAttribute{
					Name: result.Record.XattrName,
					Type: importer.ExtendedAttributeType(result.Record.XattrType),
				}
			} else {
				xattr = nil
			}

			if err := stream.Send(&importer.ScanResponse{
				Pathname: result.Record.Pathname,
				Result: &importer.ScanResponse_Record{
					Record: &importer.ScanRecord{
						Target: result.Record.Target,
						Fileinfo: &importer.ScanRecordFileInfo{
							Name:      result.Record.FileInfo.Lname,
							Size:      result.Record.FileInfo.Lsize,
							Mode:      uint32(result.Record.FileInfo.Lmode),
							ModTime:   timestamppb.New(result.Record.FileInfo.LmodTime),
							Dev:       result.Record.FileInfo.Ldev,
							Ino:       result.Record.FileInfo.Lino,
							Uid:       result.Record.FileInfo.Luid,
							Gid:       result.Record.FileInfo.Lgid,
							Nlink:     uint32(result.Record.FileInfo.Lnlink),
							Username:  result.Record.FileInfo.Lusername,
							Groupname: result.Record.FileInfo.Lgroupname,
							Flags:     result.Record.FileInfo.Flags,
						},
						FileAttributes: result.Record.FileAttributes,
						Xattr:          xattr,
					},
				},
			}); err != nil {
				return err
			}
		case result.Error != nil:
			if err := stream.Send(&importer.ScanResponse{
				Pathname: result.Error.Pathname,
				Result: &importer.ScanResponse_Error{
					Error: &importer.ScanError{
						Message: result.Error.Err.Error(),
					},
				},
			}); err != nil {
				return err
			}
		default:
			panic("?? unknown result type ??")
		}
	}
	return nil
}
func (plugin *ImporterPluginServer) Read(req *importer.ReadRequest, stream importer.Importer_ReadServer) error {
	content, err := plugin.imp.NewReader(req.Pathname)
	if err != nil {
		return err
	}
	defer content.Close()
	
	buffer := make([]byte, 8192)
	for {
		n, err := content.Read(buffer)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		
		if n > 0 {
			if err := stream.Send(&importer.ReadResponse{
				Data: buffer[:n],
			}); err != nil {
				return err
			}
		}
	}
}

func RunImporter(imp impor.Importer) error {
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

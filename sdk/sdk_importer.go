package sdk

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

	grpc_importer "github.com/PlakarKorp/go-kloset-sdk/pkg/importer"
	plakar_importer "github.com/PlakarKorp/plakar/snapshot/importer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type singleConnListener struct {
	conn net.Conn
	used bool
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.used {
		// to be replaced with cancellation
		<-make(chan struct{})
	}
	l.used = true
	return l.conn, nil
}

func (l *singleConnListener) Close() error {
	return l.conn.Close()
}

func (l *singleConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

type ImporterPluginServer struct {
	importer plakar_importer.Importer

	grpc_importer.UnimplementedImporterServer
}

func (plugin *ImporterPluginServer) Info(ctx context.Context, req *grpc_importer.InfoRequest) (*grpc_importer.InfoResponse, error) {
	return &grpc_importer.InfoResponse{
		Type:   plugin.importer.Type(),
		Origin: plugin.importer.Origin(),
		Root:   plugin.importer.Root(),
	}, nil
}

func (plugin *ImporterPluginServer) Scan(req *grpc_importer.ScanRequest, stream grpc_importer.Importer_ScanServer) error {
	scanResult, err := plugin.importer.Scan()
	if err != nil {
		return err
	}

	for result := range scanResult {
		switch {
		case result.Record != nil:
			if err := stream.Context().Err(); err != nil {
				return err
			}

			var xattr *grpc_importer.ExtendedAttribute
			if result.Record.IsXattr {
				xattr = &grpc_importer.ExtendedAttribute{
					Name: result.Record.XattrName,
					Type: grpc_importer.ExtendedAttributeType(result.Record.XattrType),
				}
			} else {
				xattr = nil
			}

			if err := stream.Send(&grpc_importer.ScanResponse{
				Pathname: result.Record.Pathname,
				Result: &grpc_importer.ScanResponse_Record{
					Record: &grpc_importer.ScanRecord{
						Target: result.Record.Target,
						Fileinfo: &grpc_importer.ScanRecordFileInfo{
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
			if err := stream.Send(&grpc_importer.ScanResponse{
				Pathname: result.Error.Pathname,
				Result: &grpc_importer.ScanResponse_Error{
					Error: &grpc_importer.ScanError{
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

func (plugin *ImporterPluginServer) Read(req *grpc_importer.ReadRequest, stream grpc_importer.Importer_ReadServer) error {
	content, err := plugin.importer.NewReader(req.Pathname)
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
			if err := stream.Send(&grpc_importer.ReadResponse{
				Data: buffer[:n],
			}); err != nil {
				return err
			}
		}
	}
}

func RunImporter(imp plakar_importer.Importer) error {
	file := os.NewFile(3, "grpc-conn")
	if file == nil {
		return fmt.Errorf("failed to get file descriptor for fd 3")
	}
	defer file.Close()

	conn, err := net.FileConn(file)
	if err != nil {
		return fmt.Errorf("failed to convert fd to net.Conn: %w", err)
	}

	listener := &singleConnListener{conn: conn}

	server := grpc.NewServer()

	grpc_importer.RegisterImporterServer(server, &ImporterPluginServer{importer: imp})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

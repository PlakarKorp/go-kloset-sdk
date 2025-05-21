package sdk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"

	grpc_exporter "github.com/PlakarKorp/go-kloset-sdk/pkg/exporter"
	"github.com/PlakarKorp/plakar/objects"
	plakar_exporter "github.com/PlakarKorp/plakar/snapshot/exporter"
	"google.golang.org/grpc"
)

type exporterPluginServer struct {
	exporter plakar_exporter.Exporter

	grpc_exporter.UnimplementedExporterServer
}

func (plugin *exporterPluginServer) Root(ctx context.Context, req *grpc_exporter.RootRequest) (*grpc_exporter.RootResponse, error) {
	return &grpc_exporter.RootResponse{
		RootPath: plugin.exporter.Root(),
	}, nil
}

func (plugin *exporterPluginServer) CreateDirectory(ctx context.Context, req *grpc_exporter.CreateDirectoryRequest) (*grpc_exporter.CreateDirectoryResponse, error) {
	err := plugin.exporter.CreateDirectory(req.Pathname)
	if err != nil {
		return nil, err
	}
	return &grpc_exporter.CreateDirectoryResponse{}, nil
}

func (plugin *exporterPluginServer) StoreFile(stream grpc_exporter.Exporter_StoreFileServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	fp := io.Reader(bytes.NewReader(req.Fp.Chunk))
	if req.Fp.Chunk == nil {
		return fmt.Errorf("no file data")
	}
	plugin.exporter.StoreFile(req.Pathname, fp, int64(req.Size))
	return stream.SendAndClose(&grpc_exporter.StoreFileResponse{})
}

func (plugin *exporterPluginServer) SetPermissions(ctx context.Context, req *grpc_exporter.SetPermissionsRequest) (*grpc_exporter.SetPermissionsResponse, error) {
	err := plugin.exporter.SetPermissions(req.Pathname, &objects.FileInfo{
		Lname : req.FileInfo.Name,
		Lsize : req.FileInfo.Size,
		Lmode : fs.FileMode(req.FileInfo.Mode),
		LmodTime : req.FileInfo.ModTime.AsTime(),
		Ldev : req.FileInfo.Dev,
		Lino : req.FileInfo.Ino,
		Luid : req.FileInfo.Uid,
		Lgid : req.FileInfo.Gid,
		Lnlink : uint16(req.FileInfo.Nlink),
		Lusername : req.FileInfo.Username,
		Lgroupname : req.FileInfo.Groupname,
		Flags : req.FileInfo.Flags,
	})
	if err != nil {
		return nil, err
	}
	return &grpc_exporter.SetPermissionsResponse{}, nil	
}

func (plugin *exporterPluginServer) Close(ctx context.Context, req *grpc_exporter.CloseRequest) (*grpc_exporter.CloseResponse, error) {
	err := plugin.exporter.Close()
	if err != nil {
		return nil, err
	}
	return &grpc_exporter.CloseResponse{}, nil
}

func RunExporter(exp plakar_exporter.Exporter) error {
	listenAddr, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 50052))
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	fmt.Printf("server listening on %s\n", listenAddr.Addr())

	grpc_exporter.RegisterExporterServer(server, &exporterPluginServer{exporter: exp})

	if err := server.Serve(listenAddr); err != nil {
		return err
	}
	return nil
}

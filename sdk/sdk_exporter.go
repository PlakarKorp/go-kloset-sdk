package sdk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"

	"github.com/PlakarKorp/kloset/objects"

	grpc_exporter "github.com/PlakarKorp/kloset/snapshot/exporter/pkg"
	plakar_exporter "github.com/PlakarKorp/kloset/snapshot/exporter"

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
	var pathname string
	var size int64
	var buf bytes.Buffer

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if pathname == "" {
			pathname = req.Pathname
			size = int64(req.Size)
		}

		if req.Fp != nil && len(req.Fp.Chunk) > 0 {
			if _, err := buf.Write(req.Fp.Chunk); err != nil {
				return err
			}
		}
	}

	if pathname == "" {
		return fmt.Errorf("no pathname provided")
	}

	if err := plugin.exporter.StoreFile(pathname, &buf, size); err != nil {
		return err
	}

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

	grpc_exporter.RegisterExporterServer(server, &exporterPluginServer{exporter: exp})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

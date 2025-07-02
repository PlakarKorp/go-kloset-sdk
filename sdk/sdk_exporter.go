package sdk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/PlakarKorp/kloset/objects"
	plakar_exporter "github.com/PlakarKorp/kloset/snapshot/exporter"
	grpc_exporter "github.com/PlakarKorp/plakar/connectors/grpc/exporter/pkg"
	"google.golang.org/grpc"
)

type exporterPluginServer struct {
	constructor plakar_exporter.ExporterFn
	exporter    plakar_exporter.Exporter

	grpc_exporter.UnimplementedExporterServer
}

func (plugin *exporterPluginServer) Init(ctx context.Context, req *grpc_exporter.InitRequest) (*grpc_exporter.InitResponse, error) {
	opts := plakar_exporter.Options{
		MaxConcurrency: uint64(req.Options.Maxconcurrency),
		// TODO std*
	}

	exp, err := plugin.constructor(ctx, &opts, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.exporter = exp
	return &grpc_exporter.InitResponse{}, nil
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
	var buf bytes.Buffer

	req, err := stream.Recv()
	if err == io.EOF {
		return fmt.Errorf("no requests received")
	}
	if err != nil {
		return err
	}

	if req.GetHeader() == nil {
		return fmt.Errorf("first request must be of type Header, got %v", req.Type)
	}

	pathname := req.GetHeader().Pathname
	size := int64(req.GetHeader().Size)

	if pathname == "" || size <= 0 {
		return fmt.Errorf("invalid pathname or size: pathname=%s, size=%d", pathname, size)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if req.GetData() != nil && len(req.GetData().Chunk) > 0 {
			if _, err := buf.Write(req.GetData().Chunk); err != nil {
				return err
			}
		}
	}

	if err := plugin.exporter.StoreFile(pathname, &buf, size); err != nil {
		return err
	}

	return stream.SendAndClose(&grpc_exporter.StoreFileResponse{})
}

func (plugin *exporterPluginServer) SetPermissions(ctx context.Context, req *grpc_exporter.SetPermissionsRequest) (*grpc_exporter.SetPermissionsResponse, error) {
	err := plugin.exporter.SetPermissions(req.Pathname, &objects.FileInfo{
		Lname:      req.FileInfo.Name,
		Lsize:      req.FileInfo.Size,
		Lmode:      fs.FileMode(req.FileInfo.Mode),
		LmodTime:   req.FileInfo.ModTime.AsTime(),
		Ldev:       req.FileInfo.Dev,
		Lino:       req.FileInfo.Ino,
		Luid:       req.FileInfo.Uid,
		Lgid:       req.FileInfo.Gid,
		Lnlink:     uint16(req.FileInfo.Nlink),
		Lusername:  req.FileInfo.Username,
		Lgroupname: req.FileInfo.Groupname,
		Flags:      req.FileInfo.Flags,
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

func RunExporter(constructor plakar_exporter.ExporterFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()

	grpc_exporter.RegisterExporterServer(server, &exporterPluginServer{
		constructor: constructor,
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

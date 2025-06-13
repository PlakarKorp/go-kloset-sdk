package sdk

import (
	"context"
	"fmt"
	kloset_importer "github.com/PlakarKorp/kloset/snapshot/importer"
	grpc_importer "github.com/PlakarKorp/plakar/connectors/grpc/importer/pkg"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"os"
	"sync"
)

type ImporterPluginServer struct {
	importer       kloset_importer.Importer
	mu             sync.Mutex
	holdingReaders map[string]io.ReadCloser

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
	scanResults, err := plugin.importer.Scan()
	if err != nil {
		return err
	}

	for result := range scanResults {

		if err := stream.Context().Err(); err != nil {
			return err
		}

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
			}


			ret := &grpc_importer.ScanResponse{
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
			}

			if result.Record.FileInfo.Mode()&os.ModeDir != 0 {
			} else {
				plugin.mu.Lock()
				plugin.holdingReaders[result.Record.Pathname] = result.Record.Reader //THE PROGRAM BLOCK HERE
				plugin.mu.Unlock()
			}
			if err := stream.Send(ret); err != nil {
				return err
			}

		case result.Error != nil:
			ret := &grpc_importer.ScanResponse{
				Pathname: result.Error.Pathname,
				Result: &grpc_importer.ScanResponse_Error{
					Error: &grpc_importer.ScanError{
						Message: result.Error.Err.Error(),
					},
				},
			}
			if err := stream.Send(ret); err != nil {
				return err
			}
		default:
			panic("?? unknown result type ??")
		}
	}
	return nil
}

func (plugin *ImporterPluginServer) Open(req *grpc_importer.OpenRequest, stream grpc_importer.Importer_OpenServer) error {
	pathname := req.Pathname


	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	reader, ok := plugin.holdingReaders[pathname]
	if !ok {
		return fmt.Errorf("no reader for pathname %s", pathname)
	}

	buf := make([]byte, 16*1024) // 16KB buffer
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&grpc_importer.OpenResponse{Chunk: buf[:n]}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (plugin *ImporterPluginServer) Close(ctx context.Context, req *grpc_importer.CloseRequest) (*grpc_importer.CloseResponse, error) {
	pathname := req.Pathname


	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	if reader, ok := plugin.holdingReaders[pathname]; ok {
		if err := reader.Close(); err != nil {
			return nil, err
		}
		delete(plugin.holdingReaders, pathname)
	}

	return &grpc_importer.CloseResponse{}, nil
}

func RunImporter(imp kloset_importer.Importer) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()
	grpc_importer.RegisterImporterServer(server, &ImporterPluginServer{
		importer:       imp,
		holdingReaders: make(map[string]io.ReadCloser),
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

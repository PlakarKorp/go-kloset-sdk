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

// ImporterPluginServer implements the gRPC server for a Plakar importer plugin.
// It wraps a local importer and exposes its methods over gRPC.
type ImporterPluginServer struct {
	// Constructor creates a new importer instance with given options.
	Constructor kloset_importer.ImporterFn

	// Importer is the active Importer instance.
	Importer kloset_importer.Importer

	// Mu protects holdingReaders from concurrent access.
	Mu sync.Mutex

	// HoldingReaders holds file readers opened by the Scan step.
	HoldingReaders map[string]io.ReadCloser

	grpc_importer.UnimplementedImporterServer
}

// Init initializes the importer with options and configuration.
func (plugin *ImporterPluginServer) Init(ctx context.Context, req *grpc_importer.InitRequest) (*grpc_importer.InitResponse, error) {
	opts := kloset_importer.Options{
		Hostname:        req.Options.Hostname,
		OperatingSystem: req.Options.Os,
		Architecture:    req.Options.Arch,
		CWD:             req.Options.Cwd,
		MaxConcurrency:  int(req.Options.Maxconcurrency),
		// TODO: stdin/out/err are missing
	}

	imp, err := plugin.Constructor(ctx, &opts, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.Importer = imp
	return &grpc_importer.InitResponse{}, nil
}

// Info returns metadata about the importer like type, origin, and root.
func (plugin *ImporterPluginServer) Info(ctx context.Context, req *grpc_importer.InfoRequest) (*grpc_importer.InfoResponse, error) {
	return &grpc_importer.InfoResponse{
		Type:   plugin.Importer.Type(),
		Origin: plugin.Importer.Origin(),
		Root:   plugin.Importer.Root(),
	}, nil
}

// Scan scans for records using the importer and streams them back.
func (plugin *ImporterPluginServer) Scan(req *grpc_importer.ScanRequest, stream grpc_importer.Importer_ScanServer) error {
	scanResults, err := plugin.Importer.Scan()
	if err != nil {
		return err
	}

	for result := range scanResults {
		if err := stream.Context().Err(); err != nil {
			return err
		}

		switch {
		case result.Record != nil:
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

			// If not a directory, store the reader for later OpenReader.
			if result.Record.FileInfo.Mode()&os.ModeDir == 0 {
				plugin.Mu.Lock()
				plugin.HoldingReaders[result.Record.Pathname] = result.Record.Reader
				plugin.Mu.Unlock()
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
			panic("unknown scan result type")
		}
	}
	return nil
}

// OpenReader streams a file's content using the stored reader.
func (plugin *ImporterPluginServer) OpenReader(req *grpc_importer.OpenReaderRequest, stream grpc_importer.Importer_OpenReaderServer) error {
	pathname := req.Pathname

	plugin.Mu.Lock()
	defer plugin.Mu.Unlock()
	reader, ok := plugin.HoldingReaders[pathname]

	if !ok {
		return fmt.Errorf("no reader for pathname %s", pathname)
	}

	buf := make([]byte, 16*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&grpc_importer.OpenReaderResponse{Chunk: buf[:n]}); err != nil {
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

// CloseReader closes an open file reader.
func (plugin *ImporterPluginServer) CloseReader(ctx context.Context, req *grpc_importer.CloseReaderRequest) (*grpc_importer.CloseReaderResponse, error) {
	pathname := req.Pathname

	plugin.Mu.Lock()
	defer plugin.Mu.Unlock()
	if reader, ok := plugin.HoldingReaders[pathname]; ok {
		if err := reader.Close(); err != nil {
			return nil, err
		}
		delete(plugin.HoldingReaders, pathname)
	}

	return &grpc_importer.CloseReaderResponse{}, nil
}

// Close finalizes the importer and closes all open readers.
func (plugin *ImporterPluginServer) Close(ctx context.Context, req *grpc_importer.CloseRequest) (*grpc_importer.CloseResponse, error) {
	plugin.Mu.Lock()
	defer plugin.Mu.Unlock()
	for _, reader := range plugin.HoldingReaders {
		_ = reader.Close()
	}
	plugin.HoldingReaders = make(map[string]io.ReadCloser)

	if err := plugin.Importer.Close(); err != nil {
		return nil, err
	}
	return &grpc_importer.CloseResponse{}, nil
}

// RunImporter starts the gRPC server for the importer plugin.
func RunImporter(constructor kloset_importer.ImporterFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()
	grpc_importer.RegisterImporterServer(server, &ImporterPluginServer{
		Constructor:    constructor,
		HoldingReaders: make(map[string]io.ReadCloser),
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

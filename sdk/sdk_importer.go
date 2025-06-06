package sdk

import (
	"context"
	"fmt"
	kloset_importer "github.com/PlakarKorp/kloset/snapshot/importer"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/pkg"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	//"log"
	"net"
	"os"
	"sync"
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
	importer       kloset_importer.Importer
	mu             sync.Mutex // mutex to protect the holdingReaders map
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
		//log.Printf("[SDK] Scan error: %v\n", err)
		return err
	}

	for result := range scanResults {
		//log.Printf("[SDK] Scan result: %v\n", result)

		if err := stream.Context().Err(); err != nil {
			//log.Printf("[SDK] Scan context error: %v\n", err)
			return err
		}

		switch {
		case result.Record != nil:
			if err := stream.Context().Err(); err != nil {
				//log.Printf("[SDK] Scan context error: %v\n", err)
				return err
			}

			var xattr *grpc_importer.ExtendedAttribute
			if result.Record.IsXattr {
				xattr = &grpc_importer.ExtendedAttribute{
					Name: result.Record.XattrName,
					Type: grpc_importer.ExtendedAttributeType(result.Record.XattrType),
				}
			}

			//log.Printf("[SDK] Sending scan result for pathname: %s, target: %s, fileinfo: %+v, xattr: %+v\n", result.Record.Pathname, result.Record.Target, result.Record.FileInfo, xattr)

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
				//log.Printf("[SDK] Directory detected, not adding to flying scan: %s\n", result.Record.Pathname)
			} else {
				//log.Printf("[SDK] stroring record reader\n")
				plugin.mu.Lock()
				plugin.holdingReaders[result.Record.Pathname] = result.Record.Reader //THE PROGRAM BLOCK HERE
				//log.Printf("[SDK] Added reader for pathname: %s, total readers: %d\n", result.Record.Pathname, len(plugin.holdingReaders))
				plugin.mu.Unlock()
			}
			if err := stream.Send(ret); err != nil {
				//log.Printf("[SDK] Error sending scan response for pathname %s: %v\n", result.Record.Pathname, err)
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
			//log.Printf("[SDK] Sending scan error for pathname: %s, error: %v\n", result.Error.Pathname, result.Error.Err)
			if err := stream.Send(ret); err != nil {
				//log.Printf("[SDK] Error sending scan error for pathname %s: %v\n", result.Error.Pathname, err)
				return err
			}
		default:
			//log.Printf("[SDK] Unknown scan result type for pathname: %s\n", result.Record.Pathname)
			panic("?? unknown result type ??")
		}
	}
	//log.Printf("[SDK] Scan completed, closing stream\n")
	return nil
}

func (plugin *ImporterPluginServer) Open(req *grpc_importer.OpenRequest, stream grpc_importer.Importer_OpenServer) error {
	pathname := req.Pathname

	//log.Printf("[SDK] Opening pathname: %s\n", pathname)

	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	reader, ok := plugin.holdingReaders[pathname]
	//log.Printf("[SDK] Holding readers: %d\n", len(plugin.holdingReaders))
	if !ok {
		return fmt.Errorf("no reader for pathname %s", pathname)
	}

	buf := make([]byte, 16*1024) // 16KB buffer
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			//log.Printf("\n[SDK] Sending chunk for pathname: %s, size: %d\n", pathname, n)
			if err := stream.Send(&grpc_importer.OpenResponse{Chunk: buf[:n]}); err != nil {
				//log.Printf("[SDK] Error sending chunk for pathname %s: %v\n", pathname, err)
				return err
			}
		}
		if err == io.EOF {
			//log.Printf("[SDK] End of file reached for pathname: %s\n", pathname)
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

	//log.Printf("[SDK] Closing pathname: %s\n", pathname)

	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	if reader, ok := plugin.holdingReaders[pathname]; ok {
		if err := reader.Close(); err != nil {
			//log.Printf("[SDK] Error closing reader for pathname %s: %v\n", pathname, err)
			return nil, err
		}
		delete(plugin.holdingReaders, pathname)
		//log.Printf("[SDK] Closed reader for pathname: %s\n", pathname)
	}
	//} else {
	//log.Printf("[SDK] No reader found for pathname: %s\n", pathname)
	//return nil, fmt.Errorf("no reader found for pathname: %s", pathname)
	//}

	return &grpc_importer.CloseResponse{}, nil
}

func RunImporter(imp kloset_importer.Importer) error {
	sockPath := "/tmp/importer.sock"
	_ = os.Remove(sockPath) // cleanup in case it exists

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("failed to create Unix socket: %w", err)
	}
	defer os.Remove(sockPath)

	server := grpc.NewServer()
	grpc_importer.RegisterImporterServer(server, &ImporterPluginServer{
		importer:       imp,
		holdingReaders: make(map[string]io.ReadCloser),
	})
	//log.Printf("[SDK] gRPC server listening on unix://%s\n", sockPath)

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

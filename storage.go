package sdk

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/kloset/objects"

	gstorage "github.com/PlakarKorp/integration-grpc/storage"
	"github.com/PlakarKorp/kloset/connectors/storage"

	"google.golang.org/grpc"
)

// storagePluginServer implements the gRPC server for a Plakar storage backend.
// It exposes a Store interface over gRPC for remote use.
type storagePluginServer struct {
	// constructor creates a new storage backend.
	constructor storage.StoreFn

	// storage is the active storage backend.
	storage storage.Store

	gstorage.UnimplementedStoreServer
}

// Init initializes the storage backend with proto and configuration.
func (plugin *storagePluginServer) Init(ctx context.Context, req *gstorage.InitRequest) (*gstorage.InitResponse, error) {
	imp, err := plugin.constructor(ctx, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.storage = imp
	return &gstorage.InitResponse{
		Origin: imp.Origin(),
		Type:   imp.Type(),
		Root:   imp.Root(),
		Flags:  uint32(imp.Flags()),
	}, nil
}

// Create creates a new storage with given configuration.
func (plugin *storagePluginServer) Create(ctx context.Context, req *gstorage.CreateRequest) (*gstorage.CreateResponse, error) {
	err := plugin.storage.Create(ctx, req.Config)
	if err != nil {
		return nil, err
	}
	return &gstorage.CreateResponse{}, nil
}

// Open opens an existing storage and returns its configuration.
func (plugin *storagePluginServer) Open(ctx context.Context, req *gstorage.OpenRequest) (*gstorage.OpenResponse, error) {
	b, err := plugin.storage.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &gstorage.OpenResponse{
		Config: b,
	}, nil
}

// Close closes the storage backend.
func (plugin *storagePluginServer) Close(ctx context.Context, req *gstorage.CloseRequest) (*gstorage.CloseResponse, error) {
	err := plugin.storage.Close(ctx)
	if err != nil {
		return nil, err
	}
	return &gstorage.CloseResponse{}, nil
}

// GetMode returns the storage mode (e.g. read-only, read-write).
func (plugin *storagePluginServer) GetMode(ctx context.Context, req *gstorage.ModeRequest) (*gstorage.ModeResponse, error) {
	mode, err := plugin.storage.Mode(ctx)
	if err != nil {
		return nil, err
	}

	return &gstorage.ModeResponse{
		Mode: int32(mode),
	}, nil
}

// GetSize returns the current storage size in bytes.
func (plugin *storagePluginServer) GetSize(ctx context.Context, req *gstorage.SizeRequest) (*gstorage.SizeResponse, error) {
	size, err := plugin.storage.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &gstorage.SizeResponse{
		Size: size,
	}, nil
}

// GetStates returns the list of macs of the requested resource.
func (plugin *storagePluginServer) List(ctx context.Context, req *gstorage.ListRequest) (*gstorage.ListResponse, error) {
	resources, err := plugin.storage.List(ctx, storage.StorageResource(req.GetType()))
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, rsrc := range resources {
		tmp := make([]byte, len(rsrc))
		copy(tmp, rsrc[:])
		out = append(out, tmp)
	}
	return &gstorage.ListResponse{
		Macs: out,
	}, nil
}

// PutState uploads a state to the storage using chunked streaming.
func (plugin *storagePluginServer) Put(stream gstorage.Store_PutServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}

	mac := objects.MAC(req.Mac)
	size, err := plugin.storage.Put(stream.Context(), storage.StorageResource(req.Type), mac, gstorage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&gstorage.PutResponse{
		BytesWritten: size,
	})
	return err
}

// GetState streams the requested state from the storage.
func (plugin *storagePluginServer) Get(req *gstorage.GetRequest, stream gstorage.Store_GetServer) error {
	mac := objects.MAC(req.Mac)
	rg := storage.Range{Offset: req.Range.Offset, Length: req.Range.Length}
	r, err := plugin.storage.Get(stream.Context(), storage.StorageResource(req.Type), mac, &rg)
	if err != nil {
		return err
	}

	_, err = gstorage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&gstorage.GetResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeleteState deletes a state from the storage.
func (plugin *storagePluginServer) Delete(ctx context.Context, req *gstorage.DeleteRequest) (*gstorage.DeleteResponse, error) {
	mac := objects.MAC(req.Mac)
	err := plugin.storage.Delete(ctx, storage.StorageResource(req.Type), mac)
	if err != nil {
		return nil, err
	}
	return &gstorage.DeleteResponse{}, nil
}

// RunStorage starts the gRPC server for the storage plugin.
func RunStorage(constructor storage.StoreFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()

	gstorage.RegisterStoreServer(server, &storagePluginServer{
		constructor: constructor,
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

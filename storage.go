package sdk

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/kloset/objects"

	gstorage "github.com/PlakarKorp/integration-grpc/v2/storage"
	"github.com/PlakarKorp/kloset/storage"

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
	storage, err := plugin.constructor(ctx, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.storage = storage
	return &gstorage.InitResponse{}, nil
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

// GetLocation returns the storage backend location string.
func (plugin *storagePluginServer) GetLocation(ctx context.Context, req *gstorage.GetLocationRequest) (*gstorage.GetLocationResponse, error) {
	location, err := plugin.storage.Location(ctx)
	if err != nil {
		return nil, err
	}

	return &gstorage.GetLocationResponse{
		Location: location,
	}, nil
}

// GetMode returns the storage mode (e.g. read-only, read-write).
func (plugin *storagePluginServer) GetMode(ctx context.Context, req *gstorage.GetModeRequest) (*gstorage.GetModeResponse, error) {
	mode, err := plugin.storage.Mode(ctx)
	if err != nil {
		return nil, err
	}

	return &gstorage.GetModeResponse{
		Mode: int32(mode),
	}, nil
}

// GetSize returns the current storage size in bytes.
func (plugin *storagePluginServer) GetSize(ctx context.Context, req *gstorage.GetSizeRequest) (*gstorage.GetSizeResponse, error) {
	size, err := plugin.storage.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &gstorage.GetSizeResponse{
		Size: size,
	}, nil
}

// GetStates returns the list of state MACs.
func (plugin *storagePluginServer) GetStates(ctx context.Context, req *gstorage.GetStatesRequest) (*gstorage.GetStatesResponse, error) {
	states, err := plugin.storage.GetStates(ctx)
	if err != nil {
		return nil, err
	}
	var statesList []*gstorage.MAC
	for _, state := range states {
		statesList = append(statesList, &gstorage.MAC{
			Value: func() []byte {
				tmp := make([]byte, len(state))
				copy(tmp, state[:])
				return tmp
			}(),
		})
	}
	return &gstorage.GetStatesResponse{
		Macs: statesList,
	}, nil
}

// PutState uploads a state to the storage using chunked streaming.
func (plugin *storagePluginServer) PutState(stream gstorage.Store_PutStateServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutState(stream.Context(), mac, gstorage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&gstorage.PutStateResponse{
		BytesWritten: size,
	})
	return err
}

// GetState streams the requested state from the storage.
func (plugin *storagePluginServer) GetState(req *gstorage.GetStateRequest, stream gstorage.Store_GetStateServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetState(stream.Context(), mac)
	if err != nil {
		return err
	}

	_, err = gstorage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&gstorage.GetStateResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeleteState deletes a state from the storage.
func (plugin *storagePluginServer) DeleteState(ctx context.Context, req *gstorage.DeleteStateRequest) (*gstorage.DeleteStateResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeleteState(ctx, mac)
	if err != nil {
		return nil, err
	}
	return &gstorage.DeleteStateResponse{}, nil
}

// GetPackfiles returns all packfile MACs.
func (plugin *storagePluginServer) GetPackfiles(ctx context.Context, req *gstorage.GetPackfilesRequest) (*gstorage.GetPackfilesResponse, error) {
	packfiles, err := plugin.storage.GetPackfiles(ctx)
	if err != nil {
		return nil, err
	}
	var packfilesList []*gstorage.MAC
	for _, packfile := range packfiles {
		packfilesList = append(packfilesList, &gstorage.MAC{
			Value: func() []byte {
				tmp := make([]byte, len(packfile))
				copy(tmp, packfile[:])
				return tmp
			}(),
		})
	}
	return &gstorage.GetPackfilesResponse{
		Macs: packfilesList,
	}, nil
}

// PutPackfile uploads a packfile using chunked streaming.
func (plugin *storagePluginServer) PutPackfile(stream gstorage.Store_PutPackfileServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutPackfile(stream.Context(), mac, gstorage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&gstorage.PutPackfileResponse{
		BytesWritten: size,
	})
	return err
}

// GetPackfile streams the requested packfile.
func (plugin *storagePluginServer) GetPackfile(req *gstorage.GetPackfileRequest, stream gstorage.Store_GetPackfileServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetPackfile(stream.Context(), mac)
	if err != nil {
		return err
	}

	_, err = gstorage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&gstorage.GetPackfileResponse{
			Chunk: chunk,
		})
	})
	return err
}

// GetPackfileBlob streams a blob portion from a packfile.
func (plugin *storagePluginServer) GetPackfileBlob(req *gstorage.GetPackfileBlobRequest, stream gstorage.Store_GetPackfileBlobServer) error {
	mac := objects.MAC(req.Mac.Value)
	offset := req.Offset
	length := req.Length
	r, err := plugin.storage.GetPackfileBlob(stream.Context(), mac, offset, length)
	if err != nil {
		return err
	}

	_, err = gstorage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&gstorage.GetPackfileBlobResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeletePackfile deletes a packfile from the storage.
func (plugin *storagePluginServer) DeletePackfile(ctx context.Context, req *gstorage.DeletePackfileRequest) (*gstorage.DeletePackfileResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeletePackfile(ctx, mac)
	if err != nil {
		return nil, err
	}
	return &gstorage.DeletePackfileResponse{}, nil
}

// GetLocks returns all lock MACs.
func (plugin *storagePluginServer) GetLocks(ctx context.Context, req *gstorage.GetLocksRequest) (*gstorage.GetLocksResponse, error) {
	locks, err := plugin.storage.GetLocks(ctx)
	if err != nil {
		return nil, err
	}
	var locksList []*gstorage.MAC
	for _, lock := range locks {
		locksList = append(locksList, &gstorage.MAC{
			Value: func() []byte {
				tmp := make([]byte, len(lock))
				copy(tmp, lock[:])
				return tmp
			}(),
		})
	}
	return &gstorage.GetLocksResponse{
		Macs: locksList,
	}, nil
}

// PutLock uploads a lock using chunked streaming.
func (plugin *storagePluginServer) PutLock(stream gstorage.Store_PutLockServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutLock(stream.Context(), mac, gstorage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&gstorage.PutLockResponse{
		BytesWritten: size,
	})
	return err
}

// GetLock streams a lock by MAC.
func (plugin *storagePluginServer) GetLock(req *gstorage.GetLockRequest, stream gstorage.Store_GetLockServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetLock(stream.Context(), mac)
	if err != nil {
		return err
	}

	_, err = gstorage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&gstorage.GetLockResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeleteLock deletes a lock from the storage.
func (plugin *storagePluginServer) DeleteLock(ctx context.Context, req *gstorage.DeleteLockRequest) (*gstorage.DeleteLockResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeleteLock(ctx, mac)
	if err != nil {
		return nil, err
	}
	return &gstorage.DeleteLockResponse{}, nil
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

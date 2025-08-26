package sdk

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/kloset/objects"

	kloset_grpc_storage "github.com/PlakarKorp/integration-grpc/storage"
	grpc_storage "github.com/PlakarKorp/integration-grpc/storage/pkg"
	kloset_storage "github.com/PlakarKorp/kloset/storage"

	"google.golang.org/grpc"
)

// storagePluginServer implements the gRPC server for a Plakar storage backend.
// It exposes a Store interface over gRPC for remote use.
type storagePluginServer struct {
	// constructor creates a new storage backend.
	constructor kloset_storage.StoreFn

	// storage is the active storage backend.
	storage kloset_storage.Store

	grpc_storage.UnimplementedStoreServer
}

// Init initializes the storage backend with proto and configuration.
func (plugin *storagePluginServer) Init(ctx context.Context, req *grpc_storage.InitRequest) (*grpc_storage.InitResponse, error) {
	storage, err := plugin.constructor(ctx, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.storage = storage
	return &grpc_storage.InitResponse{}, nil
}

// Create creates a new storage with given configuration.
func (plugin *storagePluginServer) Create(ctx context.Context, req *grpc_storage.CreateRequest) (*grpc_storage.CreateResponse, error) {
	err := plugin.storage.Create(ctx, req.Config)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.CreateResponse{}, nil
}

// Open opens an existing storage and returns its configuration.
func (plugin *storagePluginServer) Open(ctx context.Context, req *grpc_storage.OpenRequest) (*grpc_storage.OpenResponse, error) {
	b, err := plugin.storage.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.OpenResponse{
		Config: b,
	}, nil
}

// Close closes the storage backend.
func (plugin *storagePluginServer) Close(ctx context.Context, req *grpc_storage.CloseRequest) (*grpc_storage.CloseResponse, error) {
	err := plugin.storage.Close(ctx)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.CloseResponse{}, nil
}

// GetLocation returns the storage backend location string.
func (plugin *storagePluginServer) GetLocation(ctx context.Context, req *grpc_storage.GetLocationRequest) (*grpc_storage.GetLocationResponse, error) {
	location, err := plugin.storage.Location(ctx)
	if err != nil {
		return nil, err
	}

	return &grpc_storage.GetLocationResponse{
		Location: location,
	}, nil
}

// GetMode returns the storage mode (e.g. read-only, read-write).
func (plugin *storagePluginServer) GetMode(ctx context.Context, req *grpc_storage.GetModeRequest) (*grpc_storage.GetModeResponse, error) {
	mode, err := plugin.storage.Mode(ctx)
	if err != nil {
		return nil, err
	}

	return &grpc_storage.GetModeResponse{
		Mode: int32(mode),
	}, nil
}

// GetSize returns the current storage size in bytes.
func (plugin *storagePluginServer) GetSize(ctx context.Context, req *grpc_storage.GetSizeRequest) (*grpc_storage.GetSizeResponse, error) {
	size, err := plugin.storage.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &grpc_storage.GetSizeResponse{
		Size: size,
	}, nil
}

// GetStates returns the list of state MACs.
func (plugin *storagePluginServer) GetStates(ctx context.Context, req *grpc_storage.GetStatesRequest) (*grpc_storage.GetStatesResponse, error) {
	states, err := plugin.storage.GetStates(ctx)
	if err != nil {
		return nil, err
	}
	var statesList []*grpc_storage.MAC
	for _, state := range states {
		statesList = append(statesList, &grpc_storage.MAC{
			Value: func() []byte {
				tmp := make([]byte, len(state))
				copy(tmp, state[:])
				return tmp
			}(),
		})
	}
	return &grpc_storage.GetStatesResponse{
		Macs: statesList,
	}, nil
}

// PutState uploads a state to the storage using chunked streaming.
func (plugin *storagePluginServer) PutState(stream grpc_storage.Store_PutStateServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutState(stream.Context(), mac, kloset_grpc_storage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&grpc_storage.PutStateResponse{
		BytesWritten: size,
	})
	return err
}

// GetState streams the requested state from the storage.
func (plugin *storagePluginServer) GetState(req *grpc_storage.GetStateRequest, stream grpc_storage.Store_GetStateServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetState(stream.Context(), mac)
	if err != nil {
		return err
	}

	_, err = kloset_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetStateResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeleteState deletes a state from the storage.
func (plugin *storagePluginServer) DeleteState(ctx context.Context, req *grpc_storage.DeleteStateRequest) (*grpc_storage.DeleteStateResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeleteState(ctx, mac)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.DeleteStateResponse{}, nil
}

// GetPackfiles returns all packfile MACs.
func (plugin *storagePluginServer) GetPackfiles(ctx context.Context, req *grpc_storage.GetPackfilesRequest) (*grpc_storage.GetPackfilesResponse, error) {
	packfiles, err := plugin.storage.GetPackfiles(ctx)
	if err != nil {
		return nil, err
	}
	var packfilesList []*grpc_storage.MAC
	for _, packfile := range packfiles {
		packfilesList = append(packfilesList, &grpc_storage.MAC{
			Value: func() []byte {
				tmp := make([]byte, len(packfile))
				copy(tmp, packfile[:])
				return tmp
			}(),
		})
	}
	return &grpc_storage.GetPackfilesResponse{
		Macs: packfilesList,
	}, nil
}

// PutPackfile uploads a packfile using chunked streaming.
func (plugin *storagePluginServer) PutPackfile(stream grpc_storage.Store_PutPackfileServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutPackfile(stream.Context(), mac, kloset_grpc_storage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&grpc_storage.PutPackfileResponse{
		BytesWritten: size,
	})
	return err
}

// GetPackfile streams the requested packfile.
func (plugin *storagePluginServer) GetPackfile(req *grpc_storage.GetPackfileRequest, stream grpc_storage.Store_GetPackfileServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetPackfile(stream.Context(), mac)
	if err != nil {
		return err
	}

	_, err = kloset_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetPackfileResponse{
			Chunk: chunk,
		})
	})
	return err
}

// GetPackfileBlob streams a blob portion from a packfile.
func (plugin *storagePluginServer) GetPackfileBlob(req *grpc_storage.GetPackfileBlobRequest, stream grpc_storage.Store_GetPackfileBlobServer) error {
	mac := objects.MAC(req.Mac.Value)
	offset := req.Offset
	length := req.Length
	r, err := plugin.storage.GetPackfileBlob(stream.Context(), mac, offset, length)
	if err != nil {
		return err
	}

	_, err = kloset_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetPackfileBlobResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeletePackfile deletes a packfile from the storage.
func (plugin *storagePluginServer) DeletePackfile(ctx context.Context, req *grpc_storage.DeletePackfileRequest) (*grpc_storage.DeletePackfileResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeletePackfile(ctx, mac)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.DeletePackfileResponse{}, nil
}

// GetLocks returns all lock MACs.
func (plugin *storagePluginServer) GetLocks(ctx context.Context, req *grpc_storage.GetLocksRequest) (*grpc_storage.GetLocksResponse, error) {
	locks, err := plugin.storage.GetLocks(ctx)
	if err != nil {
		return nil, err
	}
	var locksList []*grpc_storage.MAC
	for _, lock := range locks {
		locksList = append(locksList, &grpc_storage.MAC{
			Value: func() []byte {
				tmp := make([]byte, len(lock))
				copy(tmp, lock[:])
				return tmp
			}(),
		})
	}
	return &grpc_storage.GetLocksResponse{
		Macs: locksList,
	}, nil
}

// PutLock uploads a lock using chunked streaming.
func (plugin *storagePluginServer) PutLock(stream grpc_storage.Store_PutLockServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutLock(stream.Context(), mac, kloset_grpc_storage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	err = stream.SendAndClose(&grpc_storage.PutLockResponse{
		BytesWritten: size,
	})
	return err
}

// GetLock streams a lock by MAC.
func (plugin *storagePluginServer) GetLock(req *grpc_storage.GetLockRequest, stream grpc_storage.Store_GetLockServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetLock(stream.Context(), mac)
	if err != nil {
		return err
	}

	_, err = kloset_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetLockResponse{
			Chunk: chunk,
		})
	})
	return err
}

// DeleteLock deletes a lock from the storage.
func (plugin *storagePluginServer) DeleteLock(ctx context.Context, req *grpc_storage.DeleteLockRequest) (*grpc_storage.DeleteLockResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeleteLock(ctx, mac)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.DeleteLockResponse{}, nil
}

// RunStorage starts the gRPC server for the storage plugin.
func RunStorage(constructor kloset_storage.StoreFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()

	grpc_storage.RegisterStoreServer(server, &storagePluginServer{
		constructor: constructor,
	})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

package sdk

import (
	"context"
	"fmt"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"

	plakar_storage "github.com/PlakarKorp/kloset/storage"
	plakar_grpc_storage "github.com/PlakarKorp/plakar/connectors/grpc/storage"
	grpc_storage "github.com/PlakarKorp/plakar/connectors/grpc/storage/pkg"

	"google.golang.org/grpc"
)

type StoragePluginServer struct {
	storage plakar_storage.Store

	grpc_storage.UnimplementedStoreServer
}

func (plugin *StoragePluginServer) Create(ctx context.Context, req *grpc_storage.CreateRequest) (*grpc_storage.CreateResponse, error) {
	err := plugin.storage.Create(kcontext.NewKContext(), req.Config)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.CreateResponse{}, nil
}

func (plugin *StoragePluginServer) Open(ctx context.Context, req *grpc_storage.OpenRequest) (*grpc_storage.OpenResponse, error) {
	b, err := plugin.storage.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.OpenResponse{
		Config: b,
	}, nil
}

func (plugin *StoragePluginServer) Close(ctx context.Context, req *grpc_storage.CloseRequest) (*grpc_storage.CloseResponse, error) {
	err := plugin.storage.Close()
	if err != nil {
		return nil, err
	}
	return &grpc_storage.CloseResponse{}, nil
}

func (plugin *StoragePluginServer) GetLocation(ctx context.Context, req *grpc_storage.GetLocationRequest) (*grpc_storage.GetLocationResponse, error) {
	location := plugin.storage.Location()
	return &grpc_storage.GetLocationResponse{
		Location: location,
	}, nil
}

func (plugin *StoragePluginServer) GetMode(ctx context.Context, req *grpc_storage.GetModeRequest) (*grpc_storage.GetModeResponse, error) {
	mode := plugin.storage.Mode()
	return &grpc_storage.GetModeResponse{
		Mode: int32(mode),
	}, nil
}

func (plugin *StoragePluginServer) GetSize(ctx context.Context, req *grpc_storage.GetSizeRequest) (*grpc_storage.GetSizeResponse, error) {
	size := plugin.storage.Size()
	return &grpc_storage.GetSizeResponse{
		Size: size,
	}, nil
}

func (plugin *StoragePluginServer) GetStates(ctx context.Context, req *grpc_storage.GetStatesRequest) (*grpc_storage.GetStatesResponse, error) {
	states, err := plugin.storage.GetStates()
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

func (plugin *StoragePluginServer) PutState(stream grpc_storage.Store_PutStateServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutState(mac, plakar_grpc_storage.ReceiveChunks(func() ([]byte, error) {
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
	if err != nil {
		return err
	}
	return nil
}

func (plugin *StoragePluginServer) GetState(req *grpc_storage.GetStateRequest, stream grpc_storage.Store_GetStateServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetState(mac)
	if err != nil {
		return err
	}

	_, err = plakar_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetStateResponse{
			Chunk: chunk,
		})
	})
	return err
}

func (plugin *StoragePluginServer) DeleteState(ctx context.Context, req *grpc_storage.DeleteStateRequest) (*grpc_storage.DeleteStateResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeleteState(mac)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.DeleteStateResponse{}, nil
}

func (plugin *StoragePluginServer) GetPackfiles(ctx context.Context, req *grpc_storage.GetPackfilesRequest) (*grpc_storage.GetPackfilesResponse, error) {
	packfiles, err := plugin.storage.GetPackfiles()
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

func (plugin *StoragePluginServer) PutPackfile(stream grpc_storage.Store_PutPackfileServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutPackfile(mac, plakar_grpc_storage.ReceiveChunks(func() ([]byte, error) {
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
	if err != nil {
		return err
	}
	return nil
}

func (plugin *StoragePluginServer) GetPackfile(req *grpc_storage.GetPackfileRequest, stream grpc_storage.Store_GetPackfileServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetPackfile(mac)
	if err != nil {
		return err
	}

	_, err = plakar_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetPackfileResponse{
			Chunk: chunk,
		})
	})
	return err
}

func (plugin *StoragePluginServer) GetPackfileBlob(req *grpc_storage.GetPackfileBlobRequest, stream grpc_storage.Store_GetPackfileBlobServer) error {
	mac := objects.MAC(req.Mac.Value)
	offset := req.Offset
	length := req.Length
	r, err := plugin.storage.GetPackfileBlob(mac, offset, length)
	if err != nil {
		return err
	}

	_, err = plakar_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetPackfileBlobResponse{
			Chunk: chunk,
		})
	})
	return err
}

func (plugin *StoragePluginServer) DeletePackfile(ctx context.Context, req *grpc_storage.DeletePackfileRequest) (*grpc_storage.DeletePackfileResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeletePackfile(mac)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.DeletePackfileResponse{}, nil
}

func (plugin *StoragePluginServer) GetLocks(ctx context.Context, req *grpc_storage.GetLocksRequest) (*grpc_storage.GetLocksResponse, error) {
	locks, err := plugin.storage.GetLocks()
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

func (plugin *StoragePluginServer) PutLock(stream grpc_storage.Store_PutLockServer) error {
	req, err := stream.Recv() // Read the first request to get the MAC
	if err != nil {
		return err
	}
	mac := objects.MAC(req.Mac.Value)

	size, err := plugin.storage.PutLock(mac, plakar_grpc_storage.ReceiveChunks(func() ([]byte, error) {
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
	if err != nil {
		return err
	}
	return nil
}

func (plugin *StoragePluginServer) GetLock(req *grpc_storage.GetLockRequest, stream grpc_storage.Store_GetLockServer) error {
	mac := objects.MAC(req.Mac.Value)
	r, err := plugin.storage.GetLock(mac)
	if err != nil {
		return err
	}

	_, err = plakar_grpc_storage.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&grpc_storage.GetLockResponse{
			Chunk: chunk,
		})
	})
	return err
}

func (plugin *StoragePluginServer) DeleteLock(ctx context.Context, req *grpc_storage.DeleteLockRequest) (*grpc_storage.DeleteLockResponse, error) {
	mac := objects.MAC(req.Mac.Value)
	err := plugin.storage.DeleteLock(mac)
	if err != nil {
		return nil, err
	}
	return &grpc_storage.DeleteLockResponse{}, nil
}

func RunStorage(storage plakar_storage.Store) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	server := grpc.NewServer()

	grpc_storage.RegisterStoreServer(server, &StoragePluginServer{storage: storage})

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

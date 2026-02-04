package sdk

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/kloset/objects"

	gstorage "github.com/PlakarKorp/integration-grpc/storage"
	"github.com/PlakarKorp/kloset/connectors/storage"

	"google.golang.org/grpc"
)

type storagePluginServer struct {
	constructor storage.StoreFn
	storage     storage.Store

	gstorage.UnimplementedStoreServer
}

func (plugin *storagePluginServer) Init(ctx context.Context, req *gstorage.InitRequest) (*gstorage.InitResponse, error) {
	storage, err := plugin.constructor(ctx, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.storage = storage
	return &gstorage.InitResponse{
		Origin: storage.Origin(),
		Type:   storage.Type(),
		Root:   storage.Root(),
		Flags:  uint32(storage.Flags()),
	}, nil
}

func (plugin *storagePluginServer) Create(ctx context.Context, req *gstorage.CreateRequest) (*gstorage.CreateResponse, error) {
	err := plugin.storage.Create(ctx, req.Config)
	if err != nil {
		return nil, err
	}
	return &gstorage.CreateResponse{}, nil
}

func (plugin *storagePluginServer) Open(ctx context.Context, req *gstorage.OpenRequest) (*gstorage.OpenResponse, error) {
	b, err := plugin.storage.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &gstorage.OpenResponse{
		Config: b,
	}, nil
}

func (plugin *storagePluginServer) Ping(ctx context.Context, req *gstorage.PingRequest) (*gstorage.PingResponse, error) {
	if err := plugin.storage.Ping(ctx); err != nil {
		return nil, err
	}
	return &gstorage.PingResponse{}, nil
}

func (plugin *storagePluginServer) Mode(ctx context.Context, req *gstorage.ModeRequest) (*gstorage.ModeResponse, error) {
	mode, err := plugin.storage.Mode(ctx)
	if err != nil {
		return nil, err
	}
	return &gstorage.ModeResponse{Mode: int32(mode)}, nil
}

func (plugin *storagePluginServer) Size(ctx context.Context, req *gstorage.SizeRequest) (*gstorage.SizeResponse, error) {
	size, err := plugin.storage.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &gstorage.SizeResponse{Size: size}, nil
}

func (plugin *storagePluginServer) List(ctx context.Context, req *gstorage.ListRequest) (*gstorage.ListResponse, error) {
	macs, err := plugin.storage.List(ctx, storage.StorageResource(req.Type))
	if err != nil {
		return nil, err
	}

	gmacs := make([][]byte, 0, len(macs))
	for i := range macs {
		gmacs = append(gmacs, macs[i][:])
	}

	return &gstorage.ListResponse{Macs: gmacs}, nil
}

func (plugin *storagePluginServer) Put(stream grpc.ClientStreamingServer[gstorage.PutRequest, gstorage.PutResponse]) error {
	// read meta first
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	var (
		res = storage.StorageResource(req.Type)
		mac = objects.MAC(req.Mac)
	)

	size, err := plugin.storage.Put(stream.Context(), res, mac, gstorage.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	return stream.SendAndClose(&gstorage.PutResponse{BytesWritten: size})
}

func (plugin *storagePluginServer) Get(req *gstorage.GetRequest, stream grpc.ServerStreamingServer[gstorage.GetResponse]) error {
	var (
		res = storage.StorageResource(req.Type)
		mac = objects.MAC(req.Mac)

		rg *storage.Range
	)

	if r := req.Range; r != nil {
		rg = &storage.Range{
			Offset: r.Offset,
			Length: r.Length,
		}
	}

	r, err := plugin.storage.Get(stream.Context(), res, mac, rg)
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

func (plugin *storagePluginServer) Delete(ctx context.Context, req *gstorage.DeleteRequest) (*gstorage.DeleteResponse, error) {
	if err := plugin.storage.Delete(ctx, storage.StorageResource(req.Type), objects.MAC(req.Mac)); err != nil {
		return nil, err
	}

	return &gstorage.DeleteResponse{}, nil
}

func (plugin *storagePluginServer) Close(ctx context.Context, req *gstorage.CloseRequest) (*gstorage.CloseResponse, error) {
	err := plugin.storage.Close(ctx)
	if err != nil {
		return nil, err
	}
	return &gstorage.CloseResponse{}, nil
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

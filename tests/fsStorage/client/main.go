package main

import (
	"context"
	"fmt"
	"github.com/PlakarKorp/go-kloset-sdk/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := "localhost:50052"
	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := store.NewStoreClient(conn)

	resp, err := client.Create(context.Background(), &store.CreateRequest{
		Config: []byte(""),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.String())

	resp2, err := client.GetSize(context.Background(), &store.GetSizeRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Size: %d bytes\n", resp2.Size)
	if resp2.Size == 0 {
		fmt.Println("No data found in the store.")
		return
	}
}

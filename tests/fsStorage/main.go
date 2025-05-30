package main

import (
	"fmt"
	"github.com/PlakarKorp/go-kloset-sdk/sdk"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/storage/backends/fs"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <location>\n", os.Args[0])
		os.Exit(1)
	}

	scanDir := os.Args[1]
	fsStorage, err := fs.NewStore(appcontext.NewAppContext(), "fs", map[string]string{"location": scanDir})
	if err != nil {
		panic(err)
	}

	if err := sdk.RunStorage(fsStorage); err != nil {
		panic(err)
	}
}

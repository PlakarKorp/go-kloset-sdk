package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/go-kloset-sdk"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/storage"
)

type Storage struct {
	location  string
	packfiles Buckets
	states    Buckets
}

func NewStore(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
	return &Storage{
		location: storeConfig["location"],
	}, nil
}

func (s *Storage) Location(ctx context.Context) (string, error) {
	return s.location, nil
}

func (s *Storage) Path(ctx context.Context, args ...string) string {
	location, _ := s.Location(ctx)
	root := strings.TrimPrefix(location, "fis://")

	args = append(args, "")
	copy(args[1:], args)
	args[0] = root

	return filepath.Join(args...)
}
func (s *Storage) Create(ctx context.Context, config []byte) error {
	dirfp, err := os.Open(s.Path(ctx))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		err = os.MkdirAll(s.Path(ctx), 0700)
		if err != nil {
			return err
		}
	} else {
		defer dirfp.Close()
		entries, err := dirfp.Readdir(1)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("directory %s is not empty", s.Path(ctx))
		}
	}

	s.packfiles = NewBuckets(s.Path(ctx, "packfiles"))
	if err := s.packfiles.Create(); err != nil {
		return err
	}

	s.states = NewBuckets(s.Path(ctx, "states"))
	if err := s.states.Create(); err != nil {
		return err
	}

	err = os.Mkdir(s.Path(ctx, "locks"), 0700)
	if err != nil {
		return err
	}

	_, err = WriteToFileAtomic(s.Path(ctx, "CONFIG"), bytes.NewReader(config))
	return err
}

func (s *Storage) Open(ctx context.Context) ([]byte, error) {
	s.packfiles = NewBuckets(s.Path(ctx, "packfiles"))
	s.states = NewBuckets(s.Path(ctx, "states"))

	rd, err := os.Open(s.Path(ctx, "CONFIG"))
	if err != nil {
		return nil, err
	}
	defer rd.Close() // do we care about err?

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *Storage) Mode(ctx context.Context) (storage.Mode, error) {
	return storage.ModeRead | storage.ModeWrite, nil
}

func (s *Storage) Size(ctx context.Context) (int64, error) {
	var size int64
	loc, _ := s.Location(ctx)
	location := strings.TrimPrefix(loc, "fis://")
	_ = filepath.WalkDir(location, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}

		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, nil
}

func (s *Storage) GetPackfiles(ctx context.Context) ([]objects.MAC, error) {
	return s.packfiles.List()
}

func (s *Storage) GetPackfile(ctx context.Context, mac objects.MAC) (io.ReadCloser, error) {
	fp, err := s.packfiles.Get(mac)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}

	return fp, nil
}

func (s *Storage) GetPackfileBlob(ctx context.Context, mac objects.MAC, offset uint64, length uint32) (io.ReadCloser, error) {
	res, err := s.packfiles.GetBlob(mac, offset, length)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}
	return res, nil
}

func (s *Storage) DeletePackfile(ctx context.Context, mac objects.MAC) error {
	return s.packfiles.Remove(mac)
}

func (s *Storage) PutPackfile(ctx context.Context, mac objects.MAC, rd io.Reader) (int64, error) {
	return s.packfiles.Put(mac, rd)
}

func (s *Storage) Close(ctx context.Context) error {
	return nil
}

/* Indexes */
func (s *Storage) GetStates(ctx context.Context) ([]objects.MAC, error) {
	return s.states.List()
}

func (s *Storage) PutState(ctx context.Context, mac objects.MAC, rd io.Reader) (int64, error) {
	return s.states.Put(mac, rd)
}

func (s *Storage) GetState(ctx context.Context, mac objects.MAC) (io.ReadCloser, error) {
	return s.states.Get(mac)
}

func (s *Storage) DeleteState(ctx context.Context, mac objects.MAC) error {
	return s.states.Remove(mac)
}

func (s *Storage) GetLocks(ctx context.Context) ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)

	locksdir, err := os.ReadDir(s.Path(ctx, "locks"))
	if err != nil {
		return nil, err
	}

	for _, lock := range locksdir {
		if !lock.Type().IsRegular() {
			continue
		}

		lockID, err := hex.DecodeString(lock.Name())
		if err != nil {
			return nil, err
		}

		if len(lockID) != 32 {
			continue
		}

		ret = append(ret, objects.MAC(lockID))
	}

	return ret, nil
}

func (s *Storage) PutLock(ctx context.Context, lockID objects.MAC, rd io.Reader) (int64, error) {
	return WriteToFileAtomicTempDir(filepath.Join(s.Path(ctx, "locks"), hex.EncodeToString(lockID[:])), rd, s.Path(ctx, ""))
}

func (s *Storage) GetLock(ctx context.Context, lockID objects.MAC) (io.ReadCloser, error) {
	fp, err := os.Open(filepath.Join(s.Path(ctx, "locks"), hex.EncodeToString(lockID[:])))
	if err != nil {
		return nil, err
	}

	return fp, nil
}

func (s *Storage) DeleteLock(ctx context.Context, lockID objects.MAC) error {
	if err := os.Remove(filepath.Join(s.Path(ctx, "locks"), hex.EncodeToString(lockID[:]))); err != nil {
		return err
	}

	return nil
}

func main() {
	if len(os.Args) != 1 {
		fmt.Printf("Usage: %s\n", os.Args[0])
		os.Exit(1)
	}

	if err := sdk.RunStorage(NewStore); err != nil {
		panic(err)
	}
}

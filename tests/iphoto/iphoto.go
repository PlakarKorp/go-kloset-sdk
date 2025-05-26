/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package main

import (
	"bufio"
	"fmt"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"github.com/PlakarKorp/go-kloset-sdk/sdk"
)

type iCloudPhotoImporter struct {
	Username string
	TempDir  string

	ino uint64
}

func NewiCloudPhotoImporter(appCtx *appcontext.AppContext, name string, config map[string]string) (importer.Importer, error) {
	directory := filepath.Join(os.TempDir(), "plakar-icloudpd")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", directory, err)
	}
	return &iCloudPhotoImporter{
		Username: config["apple_id"],
		TempDir:  directory,
	}, nil
}

func usernameWithoutDotAndAt(username string) string {
	username = strings.ReplaceAll(username, ".", "")
	username = strings.ReplaceAll(username, "@", "")
	return username
}

func authToIcloud(username string) {
	cmd := exec.Command("icloudpd", "--username", username, "--auth-only")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		fmt.Printf("failed to run icloudpd: %v\n", err)
	}
}

func checkIcloudpd() error {
	if os.Getenv("PATH") == "" {
		_ = os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/bin:/bin")
	}

	_, err := exec.LookPath("icloudpd")
	if err != nil {
		return fmt.Errorf("icloudpd is not installed or not in PATH: %w", err)
	}
	return nil
}

func (p *iCloudPhotoImporter) Scan() (<-chan *importer.ScanResult, error) {
	if err := checkIcloudpd(); err != nil {
		return nil, fmt.Errorf("icloudpd is not installed: %w", err)
	}
	configFile := os.Getenv("HOME") + "/.pyicloud/" + usernameWithoutDotAndAt(p.Username)
	if _, err := os.Stat(configFile); err != nil {
		if os.IsNotExist(err) {
			authToIcloud(p.Username)
		} else {
			return nil, fmt.Errorf("failed to stat config file %s: %w", configFile, err)
		}
	}

	results := make(chan *importer.ScanResult, 100)

	cmd2 := exec.Command("icloudpd", "--username", p.Username, "--directory", p.TempDir)

	stdoutPipe, err := cmd2.StdoutPipe()
	if err != nil {
		panic(err)
	}

	stderrPipe, err := cmd2.StderrPipe()
	if err != nil {
		panic(err)
	}

	if err := cmd2.Start(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	fi := objects.NewFileInfo(
		"/",
		0,
		0700|os.ModeDir,
		time.Now(),
		0,
		atomic.AddUint64(&p.ino, 1),
		0,
		0,
		0,
	)
	results <- importer.NewScanRecord("/", "", fi, nil)

	createdPaths := make(map[string]bool)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "Downloaded") {
				parts := strings.Split(line, "Downloaded ")
				if len(parts) == 2 {
					realFilePath := parts[1]
					filePath := strings.TrimPrefix(realFilePath, p.TempDir)

					cleanPath := filepath.Clean(filePath)

					parts := strings.Split(cleanPath, string(os.PathSeparator))

					var currentPath string
					for _, part := range parts {
						if part == "" || strings.Contains(part, "...") {
							continue
						}
						currentPath = filepath.Join(currentPath, part)
						if filePath == "/"+currentPath {
							stats, err := os.Stat(realFilePath)
							if err != nil {
								fmt.Printf("Erreur lors de la récupération des informations du fichier : %v\n", err)
								return
							}
							fi := objects.FileInfo{
								Lname:      stats.Name(),
								Lsize:      stats.Size(),
								Lmode:      stats.Mode().Perm(),
								LmodTime:   stats.ModTime(),
								Ldev:       stats.Sys().(*syscall.Stat_t).Dev,
								Lino:       stats.Sys().(*syscall.Stat_t).Ino,
								Luid:       uint64(stats.Sys().(*syscall.Stat_t).Uid),
								Lgid:       uint64(stats.Sys().(*syscall.Stat_t).Gid),
								Lnlink:     uint16(stats.Sys().(*syscall.Stat_t).Nlink),
								Lusername:  "",
								Lgroupname: "",
							}
							results <- &importer.ScanResult{
								Record: &importer.ScanRecord{
									Pathname: "/" + currentPath,
									FileInfo: fi,
								},
							}
							break
						}
						if !createdPaths[currentPath] {
							createdPaths[currentPath] = true
							fi := objects.NewFileInfo(
								filepath.Base("/"+currentPath),
								0,
								0700|os.ModeDir,
								time.Now(),
								0,
								atomic.AddUint64(&p.ino, 1),
								0,
								0,
								0,
							)
							results <- importer.NewScanRecord("/"+currentPath, "", fi, nil)
						}
					}
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			fmt.Fprintln(os.Stderr, scanner.Text())
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()
	return results, nil
}

func (p *iCloudPhotoImporter) NewReader(pathname string) (io.ReadCloser, error) {
	if pathname == "/" {
		return nil, fmt.Errorf("cannot read root directory")
	}
	if strings.HasSuffix(pathname, "/") {
		return nil, fmt.Errorf("cannot read directory")
	}
	pathname = p.TempDir + pathname

	file, err := os.Open(pathname)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", pathname, err)
	}
	return io.NopCloser(file), nil
}

func (p *iCloudPhotoImporter) NewExtendedAttributeReader(pathname string, attribute string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("extended attributes are not supported on iCloud")
}

func (p *iCloudPhotoImporter) GetExtendedAttributes(pathname string) ([]importer.ExtendedAttributes, error) {
	return nil, fmt.Errorf("extended attributes are not supported on iCloud")
}

func (p *iCloudPhotoImporter) Close() error {
	if err := os.RemoveAll(p.TempDir); err != nil {
		return fmt.Errorf("failed to remove temporary directory %s: %w", p.TempDir, err)
	}
	return nil
}

func (p *iCloudPhotoImporter) Root() string {
	return "/"
}

func (p *iCloudPhotoImporter) Origin() string {
	return "nil"
}

func (p *iCloudPhotoImporter) Type() string {
	return "iphoto"
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <scan-dir>\n", os.Args[0])
		os.Exit(1)
	}

	argStr := os.Args[1]
	argStr = strings.TrimPrefix(argStr, "map[")
	argStr = strings.TrimSuffix(argStr, "]")
	scanMap := make(map[string]string)
	for _, pair := range strings.Fields(argStr) {
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) == 2 {
			scanMap[kv[0]] = kv[1]
		}
	}
	icloudImporter, err := NewiCloudPhotoImporter(appcontext.NewAppContext(), "iphoto", scanMap)
	if err != nil {
		panic(err)
	}
	if err := sdk.RunImporter(icloudImporter); err != nil {
		panic(err)
	}
}


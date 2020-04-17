// Package testutils provides testing utilities for testing watermill-crdb.
package testutils

import (
	"bytes"
	"context"
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

var config = struct {
	cockroachBinary string
	pgURL           string
}{}

func init() {
	flag.CommandLine.StringVar(&config.cockroachBinary, "cockroach-binary",
		"cockroach", "binary to use for cockroach, used if pgurl is "+
			"not provided")
	flag.CommandLine.StringVar(&config.pgURL, "pgurl", "",
		"if set, used for testing")
}

// MaybeStartCockroachCluster will inspect the flags and either return the
// pgurl defined by that flag or start a cockroach cluster for testing using
// the appropriate binary.
func MaybeStartCockroachCluster() (pgURL string, cleanup func(), err error) {
	flag.Parse()
	if config.pgURL != "" {
		return config.pgURL, noop, nil
	}
	cockroach, err := exec.LookPath(config.cockroachBinary)
	if err != nil {
		return "", nil, err
	}
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", nil, err
	}
	listenURLFilePath := filepath.Join(dir, "listen-url")
	cmd := exec.Command(cockroach,
		"start-single-node",
		"--insecure",
		"--listen-addr", "0.0.0.0:0",
		"--store=type=mem,size=10%",
		"--listening-url-file", listenURLFilePath)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	cleanup = func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = os.RemoveAll(dir)
	}
	if err := waitForFile(listenURLFilePath, 10*time.Second); err != nil {
		cleanup()
		return "", nil, err
	}
	url, err := ioutil.ReadFile(listenURLFilePath)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return string(bytes.TrimSpace(url)), cleanup, nil
}

func noop() {}

func waitForFile(path string, timeout time.Duration) error {
	const wait = 100 * time.Millisecond
	for deadline := time.Now().Add(timeout); time.Until(deadline) > 0; {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			time.Sleep(wait)
		} else {
			return err
		}
	}
	return context.DeadlineExceeded
}

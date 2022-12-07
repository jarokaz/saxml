/*  */ // Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package cloud defines and registers a Cloud environment.
package cloud

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"flag"
	log "github.com/golang/glog"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"golang.org/x/oauth2/google"
	"saxml/common/errors"
	"saxml/common/platform/env"
)

const (
	// sax_root flag values with this prefix are interpreted as Google Cloud Storage paths.
	gcsURLPrefix = "gs://"
	// Interally, sax_root flag values with "gs://..." URLs inside are converted to "/cns/..." paths,
	// so we can handle them uniformly as file paths without needing to import the net/url package.
	gcsPathPrefix = "/gcs/"

	// Because GCS has no real directories, put an empty placeholder file in the innermost
	// subdirectory to achieve the effect of a directory.
	metadataFile = "METADATA"
)

var (
	saxRoot  = flag.String("sax_root", "", "Sax cell root, e.g. /local/dir or gs://bucket/dir")
	testRoot = filepath.Join(os.TempDir(), "sax-test-root")

	projectID string
	gcsClient *storage.Client

	muLeader sync.Mutex
)

func init() {
	env.Register(new(Env))

	// Initialize the Cloud API.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	credentials, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error retrieving Google Cloud credentials, can only access local files from now on: %v", err)
		return
	}
	projectID = credentials.ProjectID

	gcsClient, err = storage.NewClient(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating a Google Cloud Storage client, further access will fail: %v", err)
		return
	}
}

func gcsBucketAndObject(ctx context.Context, path string) (*storage.BucketHandle, *storage.ObjectHandle, error) {
	if gcsClient == nil {
		return nil, nil, fmt.Errorf("no Google Cloud Storage connection: %w", errors.ErrFailedPrecondition)
	}
	path = strings.TrimPrefix(path, gcsPathPrefix)
	bucketName, objectPath, found := strings.Cut(path, "/")
	if !found {
		return nil, nil, fmt.Errorf("invalid GCS file path %v: %w", path, errors.ErrInvalidArgument)
	}
	bucket := gcsClient.Bucket(bucketName)
	object := bucket.Object(objectPath)
	return bucket, object, nil
}

// Env implements env.Env in Cloud environments.
type Env struct{}

// Init initializes the platform.
func (e *Env) Init(ctx context.Context) {
	flag.Parse()
}

// ReadFile reads the content of a file.
func (e *Env) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if strings.HasPrefix(path, gcsPathPrefix) {
		_, object, err := gcsBucketAndObject(ctx, path)
		if err != nil {
			return nil, err
		}

		r, err := object.NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("error reading GCS file %v: %w", path, err)
		}
		defer r.Close()
		return io.ReadAll(r)
	}

	return os.ReadFile(path)
}

// ReadFile reads the content of a file, caching the result on repeated reads if possible.
func (e *Env) ReadCachedFile(ctx context.Context, path string) ([]byte, error) {
	return e.ReadFile(ctx, path)
}

// WriteFile writes the content of a file.
func (e *Env) WriteFile(ctx context.Context, path string, data []byte) error {
	if strings.HasPrefix(path, gcsPathPrefix) {
		_, object, err := gcsBucketAndObject(ctx, path)
		if err != nil {
			return err
		}

		w := object.NewWriter(ctx)
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("error writing GCS file %v: %w", path, err)
		}
		return w.Close()
	}

	return os.WriteFile(path, data, 0644)
}

// WriteFileAtomically writes the content of a file safety to file systems without versioning.
func (e *Env) WriteFileAtomically(ctx context.Context, path string, data []byte) error {
	if strings.HasPrefix(path, gcsPathPrefix) {
		return e.WriteFile(ctx, path, data)
	}

	// Write to a temp file first and then rename, to reduce the risk of corrupted files.
	// Insert randomness into the file name to defend against concurrent write calls.
	tempPath := path + fmt.Sprintf(".%d.%016x", time.Now().UnixNano(), rand.Uint64())
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

// FileExists checks the existence of a file.
func (e *Env) FileExists(ctx context.Context, path string) (bool, error) {
	if strings.HasPrefix(path, gcsPathPrefix) {
		_, object, err := gcsBucketAndObject(ctx, path)
		if err != nil {
			return false, err
		}

		_, err = object.Attrs(ctx)
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}

	fileInfo, err := os.Stat(path)
	if err == nil {
		if fileInfo.IsDir() {
			return false, fmt.Errorf("%s is a directory, not a file: %w", path, errors.ErrFailedPrecondition)
		}
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// RootDir returns the directory path where all Sax cells store their metadata.
//
// On Cloud, this can be either a local file system path (e.g. /home/user/sax-root/) or a Google
// Cloud Storage URL (e.g. gs://bucket/sax-root/). Note the trailing slash is required.
func (e *Env) RootDir(ctx context.Context) string {
	if strings.Contains(os.Args[0], "_test") {
		return testRoot
	}
	if *saxRoot != "" {
		if strings.HasPrefix(*saxRoot, gcsURLPrefix) {
			return gcsPathPrefix + strings.TrimPrefix(*saxRoot, gcsURLPrefix)
		}
		return *saxRoot
	}
	saxRoot := os.Getenv("SAX_ROOT") // for when the location wrapper is embedded in the model server
	if saxRoot != "" {
		if strings.HasPrefix(saxRoot, gcsURLPrefix) {
			return gcsPathPrefix + strings.TrimPrefix(saxRoot, gcsURLPrefix)
		}
		return saxRoot
	}
	log.Fatal("Neither the sax_root flag nor the SAX_ROOT environment variable is set")
	return ""
}

// CreateDir creates a directory.
func (e *Env) CreateDir(ctx context.Context, path, acl string) error {
	if acl != "" {
		return fmt.Errorf("CreateDir with ACL is not supported: %w", errors.ErrUnimplemented)
	}
	if strings.HasPrefix(path, gcsPathPrefix) {
		_, object, err := gcsBucketAndObject(ctx, filepath.Join(path, metadataFile))
		if err != nil {
			return err
		}

		w := object.NewWriter(ctx)
		return w.Close()
	}

	return os.MkdirAll(path, 0777)
}

// ListSubdirs lists subdirectories in a directory.
func (e *Env) ListSubdirs(ctx context.Context, path string) ([]string, error) {
	if strings.HasPrefix(path, gcsPathPrefix) {
		bucket, _, err := gcsBucketAndObject(ctx, path)
		if err != nil {
			return nil, err
		}

		query := &storage.Query{Prefix: path, Delimiter: "/"}
		var subDirs []string
		it := bucket.Objects(ctx, query)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			subDirs = append(subDirs, attrs.Name)
		}
		return subDirs, nil
	}

	dirEntries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, dirEntry := range dirEntries {
		dirs = append(dirs, dirEntry.Name())
	}
	return dirs, nil
}

// DirExists checks the existence of a directory.
func (e *Env) DirExists(ctx context.Context, path string) (bool, error) {
	if strings.HasPrefix(path, gcsPathPrefix) {
		_, object, err := gcsBucketAndObject(ctx, filepath.Join(path, metadataFile))
		if err != nil {
			return false, err
		}

		_, err = object.Attrs(ctx)
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}

	fileInfo, err := os.Stat(path)
	if err == nil {
		if fileInfo.IsDir() {
			return true, nil
		}
		return false, fmt.Errorf("%s is a file, not a directory: %w", path, errors.ErrFailedPrecondition)
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Watch watches for content changes in a file and sends the new content on the returned channel.
func (e *Env) Watch(ctx context.Context, path string) (<-chan []byte, error) {
	// Return a no-op channel because we don't support watching address changes yet.
	return make(<-chan []byte), nil
}

// Lead blocks until it acquires exclusive access to a file. The caller should arrange calling
// close() on the returned channel to release the exclusive lock.
func (e *Env) Lead(ctx context.Context, path string) (chan<- struct{}, error) {
	// We don't support cross-process, file lock-based leader election yet.
	// This in-process implementation makes the unit test pass.
	muLeader.Lock()
	closer := make(chan struct{})
	go func() {
		<-closer
		muLeader.Unlock()
	}()
	return closer, nil
}

// PickUnusedPort picks an unused port.
func (e *Env) PickUnusedPort() (port int, err error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// DialContext establishes a gRPC connection to the target.
func (e *Env) DialContext(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, target, grpc.WithInsecure())
}

// RequiredACLNamePrefix returns the string required to prefix all ACL names.
func (e *Env) RequiredACLNamePrefix() string {
	return ""
}

// Server extends the grpc server type with a GRPCServer method.
type Server struct {
	*grpc.Server
}

// GRPCServer returns the underlying gRPC server.
func (s *Server) GRPCServer() *grpc.Server {
	return s.Server
}

// CheckACLs returns nil iff the principal extracted from ctx passes an ACL check.
func (s *Server) CheckACLs(ctx context.Context, acls []string) error {
	if len(acls) == 0 {
		return nil
	}
	return fmt.Errorf("ACL check is not supported: %w", errors.ErrUnimplemented)
}

// NewServer creates a gRPC server.
func (e *Env) NewServer() (env.Server, error) {
	s := &Server{grpc.NewServer()}
	reflection.Register(s.GRPCServer())
	return s, nil
}

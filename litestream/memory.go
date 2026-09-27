package litestream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	ls "github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// memoryObject is one stored LTX file.
type memoryObject struct {
	data      []byte
	createdAt time.Time
}

// memoryObjects is the process-global in-memory replica store. Every memory
// ReplicaClient shares it, so separate runtimes (and their read and write
// VFS instances) in one process observe the same replica — the in-process
// equivalent of two API instances sharing a bucket. It is lost on exit.
// Give each test run a unique root_prefix to avoid collisions.
var (
	memoryMu      sync.RWMutex
	memoryObjects = map[string]memoryObject{}
)

// MemoryReplicaClient is an in-process litestream.ReplicaClient for the
// "memory" provider. It exercises the full runtime/VFS/lease/flush path with
// no object storage and no network, for unit tests and local development.
type MemoryReplicaClient struct {
	path   string
	logger *slog.Logger
}

var _ ls.ReplicaClient = (*MemoryReplicaClient)(nil)

// NewMemoryReplicaClient builds a client rooted at path (the database's
// replica prefix).
func NewMemoryReplicaClient(path string) *MemoryReplicaClient {
	return &MemoryReplicaClient{path: path, logger: slog.Default().WithGroup("memory")}
}

// Type returns "memory".
func (c *MemoryReplicaClient) Type() string { return "memory" }

// Init is a no-op; there is no remote connection to establish.
func (c *MemoryReplicaClient) Init(context.Context) error { return nil }

// SetLogger sets the client logger.
func (c *MemoryReplicaClient) SetLogger(logger *slog.Logger) {
	c.logger = logger.WithGroup("memory")
}

// LTXFiles iterates the stored files for a level, ascending by TXID.
func (c *MemoryReplicaClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	prefix := ls.LTXLevelDir(c.path, level) + "/"
	memoryMu.RLock()
	infos := make([]*ltx.FileInfo, 0)
	for key, obj := range memoryObjects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		minTXID, maxTXID, err := ltx.ParseFilename(strings.TrimPrefix(key, prefix))
		if err != nil || minTXID < seek {
			continue
		}
		infos = append(infos, &ltx.FileInfo{
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      int64(len(obj.data)),
			CreatedAt: obj.createdAt,
		})
	}
	memoryMu.RUnlock()
	return ltx.NewFileInfoSliceIterator(infos), nil
}

// OpenLTXFile returns a reader for the stored file. The offset and size
// windowing mirrors the file provider.
func (c *MemoryReplicaClient) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	key := ls.LTXFilePath(c.path, level, minTXID, maxTXID)
	memoryMu.RLock()
	obj, ok := memoryObjects[key]
	memoryMu.RUnlock()
	if !ok {
		return nil, ls.NewLTXError("open", key, level, uint64(minTXID), uint64(maxTXID), os.ErrNotExist)
	}
	data := obj.data
	if offset > 0 {
		if offset >= int64(len(data)) {
			data = nil
		} else {
			data = data[offset:]
		}
	}
	if size > 0 && int64(len(data)) > size {
		data = data[:size]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// WriteLTXFile stores one LTX file and records its header timestamp.
func (c *MemoryReplicaClient) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	hdr, _, err := ltx.PeekHeader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	createdAt := time.UnixMilli(hdr.Timestamp).UTC()
	key := ls.LTXFilePath(c.path, level, minTXID, maxTXID)
	memoryMu.Lock()
	memoryObjects[key] = memoryObject{data: data, createdAt: createdAt}
	memoryMu.Unlock()
	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      int64(len(data)),
		CreatedAt: createdAt,
	}, nil
}

// DeleteLTXFiles removes the named files, ignoring absent ones.
func (c *MemoryReplicaClient) DeleteLTXFiles(_ context.Context, a []*ltx.FileInfo) error {
	memoryMu.Lock()
	defer memoryMu.Unlock()
	for _, info := range a {
		delete(memoryObjects, ls.LTXFilePath(c.path, info.Level, info.MinTXID, info.MaxTXID))
	}
	return nil
}

// DeleteAll removes every file under this client's prefix.
func (c *MemoryReplicaClient) DeleteAll(context.Context) error {
	prefix := c.path + "/"
	memoryMu.Lock()
	defer memoryMu.Unlock()
	for key := range memoryObjects {
		if strings.HasPrefix(key, prefix) {
			delete(memoryObjects, key)
		}
	}
	return nil
}

package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"walrusd/identity"
	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
	"walrusd/walrusderr"
)

func class(err error) string { return string(walrusderr.ClassOf(err)) }

func newTestRuntime(t *testing.T, owner string) *runtime.Runtime {
	t.Helper()
	store := lease.NewMemoryStore()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	// Most tests target one write attempt. Retry-specific tests opt back in.
	cfg.RetryPolicy = runtime.RetryPolicy{}
	rt, err := runtime.New(store, owner, cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return rt
}

func descriptor(t *testing.T, id string) runtime.DatabaseDescriptor {
	t.Helper()
	return runtime.DatabaseDescriptor{
		DatabaseID: id,
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: t.TempDir(),
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
}

func TestWithWriteRequiresIdempotencyKey(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/user_1")
	_, err := rt.WithWrite(context.Background(), d, "", func(conn *sql.Conn) error { return nil })
	if class(err) != "DB_INVALID_ARGUMENT" {
		t.Fatalf("class = %q, want DB_INVALID_ARGUMENT", class(err))
	}
}

func TestWithWriteLifecycle(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/user_1")

	res, err := rt.WithWrite(context.Background(), d, "op_1", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(), `
			CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)
		`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(),
			`INSERT INTO events (id, body) VALUES (1, 'hello')`)
		return err
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.TXID == "" {
		t.Fatal("expected TXID after flush")
	}

	// Read back from a fresh read session: sees only flushed remote state.
	var body string
	err = rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(),
			`SELECT body FROM events WHERE id = 1`).Scan(&body)
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if body != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
}

func TestWithWriteSecondWriterWaits(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/user_1")

	// Hold the lease externally to simulate a concurrent writer.
	db, _ := identity.NewDatabaseID(d.DatabaseID)
	_ = rt
	store := lease.NewMemoryStore()
	lm := lease.NewManager(store, "api-other", lease.DefaultConfig(), nil)
	held, err := lm.Acquire(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Release(context.Background(), held)

	// A runtime sharing the same store sees DB_BUSY.
	shared, err := runtime.New(store, "api-2", runtime.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = shared.WithWrite(ctx, d, "op_2", func(conn *sql.Conn) error { return nil })
	if err == nil {
		t.Fatal("expected busy error while lease held")
	}
	if !errors.Is(err, context.DeadlineExceeded) && class(err) != "DB_BUSY" {
		t.Fatalf("want DB_BUSY (or ctx deadline), got %v", err)
	}
}

func TestWriteAfterReleaseSeesFlushedState(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/user_1")

	if _, err := rt.WithWrite(context.Background(), d, "op_a", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS t (v TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if _, err := rt.WithWrite(context.Background(), d, "op_b", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `INSERT INTO t (v) VALUES ('second')`)
		return err
	}); err != nil {
		t.Fatalf("write2: %v", err)
	}
	var count int
	if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM t`).Scan(&count)
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestLeaseKeysIncludeRootPrefix(t *testing.T) {
	const databaseID = "shared/u1"

	store := lease.NewMemoryStore()
	rt, err := runtime.New(store, "api-1", runtime.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	first := runtime.DatabaseDescriptor{
		DatabaseID: databaseID,
		Storage: litestream.Profile{
			Provider:   "file",
			RootPrefix: "tenants/acme",
			FileRoot:   root,
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
	second := runtime.DatabaseDescriptor{
		DatabaseID: databaseID,
		Storage: litestream.Profile{
			Provider:   "file",
			RootPrefix: "tenants/other",
			FileRoot:   root,
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}

	db, err := identity.NewDatabaseID(databaseID)
	if err != nil {
		t.Fatal(err)
	}
	manager := lease.NewManager(store, "api-other", lease.DefaultConfig(), nil)
	held, err := manager.Acquire(context.Background(), db, first.Storage.RootPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Release(context.Background(), held)

	if _, err := rt.WithWrite(context.Background(), second, "write", func(*sql.Conn) error {
		return nil
	}); err != nil {
		t.Fatalf("write with different root prefix: %v", err)
	}

	for _, prefix := range []string{first.Storage.RootPrefix, second.Storage.RootPrefix} {
		body, _, err := store.Get(context.Background(), db.LeaseKey(prefix))
		if err != nil {
			t.Fatalf("read lease under %q: %v", prefix, err)
		}
		var rec lease.Record
		if err := json.Unmarshal(body, &rec); err != nil {
			t.Fatal(err)
		}
		if rec.DatabaseID != databaseID {
			t.Fatalf("database_id under %q = %q, want %q", prefix, rec.DatabaseID, databaseID)
		}
	}
	if _, _, err := store.Get(context.Background(), db.LeaseKey("")); !errors.Is(err, lease.ErrNotFound) {
		t.Fatalf("unscoped lease key exists: %v", err)
	}
}

// The memory provider runs the entire write/flush/read path in-process with
// no object storage and no Redis: two runtimes sharing one memory lease
// store behave like two API instances sharing a bucket.
func TestMemoryProviderEndToEnd(t *testing.T) {
	store := lease.NewMemoryStore()
	prefix := fmt.Sprintf("memory-e2e-%d", time.Now().UnixNano())
	d := runtime.DatabaseDescriptor{
		DatabaseID:  "users/memory",
		Storage:     litestream.Profile{Provider: "memory", RootPrefix: prefix},
		Credentials: runtime.StaticCredentials{},
	}
	newInstance := func(owner string) *runtime.Runtime {
		t.Helper()
		cfg := runtime.DefaultConfig()
		cfg.RetryPolicy = runtime.RetryPolicy{}
		rt, err := runtime.New(store, owner, cfg)
		if err != nil {
			t.Fatalf("new runtime %s: %v", owner, err)
		}
		t.Cleanup(func() { _ = rt.Close() })
		return rt
	}
	ctx := context.Background()
	a := newInstance("api-1")
	b := newInstance("api-2")

	if _, err := a.WithWrite(ctx, d, "schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("schema write: %v", err)
	}
	res, err := a.WithWrite(ctx, d, "set-greeting", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO kv (k, v) VALUES ('greeting', 'hello memory')`)
		return err
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.TXID == "" {
		t.Fatal("expected a flushed TXID")
	}

	// A second instance with the same store and replica dedupes the retry.
	dup, err := b.WithWrite(ctx, d, "set-greeting", func(*sql.Conn) error { return nil })
	if err != nil {
		t.Fatalf("dedupe write: %v", err)
	}
	if !dup.Deduplicated || dup.TXID != res.TXID {
		t.Fatalf("dedupe = %+v, want txid %s deduplicated", dup, res.TXID)
	}

	// And it reads the flushed remote state.
	var got string
	if err := b.WithRead(ctx, d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT v FROM kv WHERE k = 'greeting'`).Scan(&got)
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "hello memory" {
		t.Fatalf("read = %q, want %q", got, "hello memory")
	}
}

// Failures must name the content database so a multi-tenant operator can
// tell which one broke.
func TestErrorsNameDatabase(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := runtime.DatabaseDescriptor{
		DatabaseID:  "users/named",
		Storage:     litestream.Profile{Provider: "bogus"},
		Credentials: runtime.StaticCredentials{},
	}
	_, err := rt.WithWrite(context.Background(), d, "op", func(*sql.Conn) error { return nil })
	if err == nil {
		t.Fatal("expected unsupported provider to fail")
	}
	if got := class(err); got != "DB_CONFIGURATION_INVALID" {
		t.Fatalf("class = %q, want DB_CONFIGURATION_INVALID", got)
	}
	var we *walrusderr.Error
	if !errors.As(err, &we) {
		t.Fatalf("error is not a walrusderr.Error: %v", err)
	}
	if we.DatabaseID() != "users/named" {
		t.Fatalf("DatabaseID() = %q, want users/named", we.DatabaseID())
	}
	if !strings.Contains(err.Error(), `database_id "users/named"`) {
		t.Fatalf("message does not name the database: %v", err)
	}

	if err := rt.WithRead(context.Background(), d, func(*sql.Conn) error { return nil }); err == nil {
		t.Fatal("expected read to fail too")
	} else if !strings.Contains(err.Error(), `database_id "users/named"`) {
		t.Fatalf("read error does not name the database: %v", err)
	}
}

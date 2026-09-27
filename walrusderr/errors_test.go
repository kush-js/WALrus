package walrusderr_test

import (
	"errors"
	"testing"

	"walrusd/walrusderr"
)

func TestClassOf(t *testing.T) {
	err := walrusderr.New(walrusderr.ClassInvalidArgument, "bad request")
	if got := walrusderr.ClassOf(err); got != walrusderr.ClassInvalidArgument {
		t.Fatalf("ClassOf() = %q, want %q", got, walrusderr.ClassInvalidArgument)
	}
	if got := walrusderr.ClassOf(errors.New("plain")); got != "" {
		t.Fatalf("ClassOf(plain) = %q, want empty", got)
	}
}

func TestWrapPreservesCause(t *testing.T) {
	cause := errors.New("underlying")
	err := walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "read replica", cause)
	if !errors.Is(err, cause) {
		t.Fatal("wrapped error does not preserve its cause")
	}
	if got := walrusderr.ClassOf(err); got != walrusderr.ClassRemoteUnavailable {
		t.Fatalf("ClassOf() = %q, want %q", got, walrusderr.ClassRemoteUnavailable)
	}
	want := "DB_REMOTE_UNAVAILABLE: read replica: underlying"
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestWithDatabase(t *testing.T) {
	busy := walrusderr.Busy("lease held", 250)
	annotated := walrusderr.WithDatabase(busy, "users/u1")
	if got := walrusderr.ClassOf(annotated); got != walrusderr.ClassBusy {
		t.Fatalf("ClassOf() = %q, want %q", got, walrusderr.ClassBusy)
	}
	if hint, ok := annotated.(walrusderr.RetryAfterHint); !ok {
		t.Fatal("annotated error lost its Retry-After hint")
	} else if ms, ok := hint.RetryAfter(); !ok || ms != 250 {
		t.Fatalf("RetryAfter() = %d, %v; want 250, true", ms, ok)
	}
	want := `DB_BUSY: database_id "users/u1": lease held`
	if got := annotated.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	// The original error is not mutated, and re-annotating is idempotent.
	if got := busy.Error(); got != "DB_BUSY: lease held" {
		t.Fatalf("original mutated: %q", got)
	}
	if again := walrusderr.WithDatabase(annotated, "users/u1"); again != annotated {
		t.Fatalf("re-annotation returned a new error: %v", again)
	}

	// Non-classified errors keep their chain.
	plain := errors.New("driver blew up")
	wrapped := walrusderr.WithDatabase(plain, "users/u1")
	if !errors.Is(wrapped, plain) {
		t.Fatal("wrapped non-walrusd error lost its cause")
	}
	if got := wrapped.Error(); got != `database_id "users/u1": driver blew up` {
		t.Fatalf("Error() = %q", got)
	}
}

func TestRetryAfterHint(t *testing.T) {
	var retryErr walrusderr.RetryAfterHint = walrusderr.Busy("database is busy", 250)
	if got, ok := retryErr.RetryAfter(); !ok || got != 250 {
		t.Fatalf("RetryAfter() = %d, %v; want 250, true", got, ok)
	}

	for _, err := range []*walrusderr.Error{
		walrusderr.New(walrusderr.ClassBusy, "busy"),
		walrusderr.Busy("busy", 0),
		walrusderr.New(walrusderr.ClassConflict, "conflict"),
	} {
		if got, ok := err.RetryAfter(); ok || got != 0 {
			t.Fatalf("RetryAfter() = %d, %v; want 0, false", got, ok)
		}
	}
}

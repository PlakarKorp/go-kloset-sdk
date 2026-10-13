package sdk

import (
	"errors"
	"io"
	"testing"
)

// payloadPipe returns the body' pipe taken in the given order.
func payloadPipe(t *testing.T, ps *payloads, id uint64, writerFirst bool) (*io.PipeReader, *io.PipeWriter) {
	t.Helper()

	var (
		rd  *io.PipeReader
		wr  *io.PipeWriter
		err error
	)

	if writerFirst {
		if wr, err = ps.writer(id); err == nil {
			rd, err = ps.reader(id)
		}
	} else {
		if rd, err = ps.reader(id); err == nil {
			wr, err = ps.writer(id)
		}
	}
	if err != nil {
		t.Fatalf("claim payload %d: %v", id, err)
	}

	return rd, wr
}

// Whichever stream gets there first, both end up with the two ends of
// the same pipe.
func TestPayloadsClaimInEitherOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		writerFirst bool
	}{
		{"record first", false},
		{"payload first", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ps payloads
			rd, wr := payloadPipe(t, &ps, 7, tc.writerFirst)

			go func() {
				wr.Write([]byte("body"))
				wr.Close()
			}()

			got, err := io.ReadAll(rd)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if string(got) != "body" {
				t.Errorf("got %q, want %q", got, "body")
			}
		})
	}
}

func TestPayloadsKeepsIdsApart(t *testing.T) {
	var ps payloads

	rd1, wr1 := payloadPipe(t, &ps, 1, false)
	rd2, wr2 := payloadPipe(t, &ps, 2, false)

	go func() {
		wr1.Write([]byte("one"))
		wr1.Close()
		wr2.Write([]byte("two"))
		wr2.Close()
	}()

	for _, tc := range []struct {
		rd   io.Reader
		want string
	}{{rd1, "one"}, {rd2, "two"}} {
		got, err := io.ReadAll(tc.rd)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
}

// A client that gets the same payload twice on one stream is broken.
func TestPayloadsRejectsTheSameHalfTwice(t *testing.T) {
	var ps payloads

	if _, err := ps.reader(1); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := ps.reader(1); !errors.Is(err, ErrDuplicatePayload) {
		t.Errorf("second claim: %v, want %v", err, ErrDuplicatePayload)
	}

	if _, err := ps.writer(2); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := ps.writer(2); !errors.Is(err, ErrDuplicatePayload) {
		t.Errorf("second claim: %v, want %v", err, ErrDuplicatePayload)
	}
}

func TestPayloadsFailCancelPendingPayloads(t *testing.T) {
	boom := errors.New("session gone")

	t.Run("blocked read", func(t *testing.T) {
		var ps payloads
		rd, err := ps.reader(1)
		if err != nil {
			t.Fatalf("reader: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := rd.Read(make([]byte, 8))
			done <- err
		}()

		ps.fail(boom)
		if err := <-done; !errors.Is(err, boom) {
			t.Errorf("read returned %v, want %v", err, boom)
		}
	})

	t.Run("blocked write", func(t *testing.T) {
		var ps payloads
		wr, err := ps.writer(1)
		if err != nil {
			t.Fatalf("writer: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := wr.Write([]byte("body"))
			done <- err
		}()

		ps.fail(boom)
		if err := <-done; !errors.Is(err, boom) {
			t.Errorf("write returned %v, want %v", err, boom)
		}
	})
}

func TestPayloadsFailLeavesClaimedBodiesAlone(t *testing.T) {
	var ps payloads
	rd, wr := payloadPipe(t, &ps, 1, false)

	ps.fail(errors.New("session gone"))

	go func() {
		wr.Write([]byte("body"))
		wr.Close()
	}()

	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "body" {
		t.Errorf("got %q, want %q", got, "body")
	}
}

// A PutData stream still arriving after the session is over gets told
// so, rather than filling a pipe nobody will ever read.
func TestPayloadsRejectsClaimsAfterFail(t *testing.T) {
	boom := errors.New("session gone")

	var ps payloads
	ps.fail(boom)

	if _, err := ps.reader(1); !errors.Is(err, boom) {
		t.Errorf("reader: %v, want %v", err, boom)
	}
	if _, err := ps.writer(1); !errors.Is(err, boom) {
		t.Errorf("writer: %v, want %v", err, boom)
	}
}

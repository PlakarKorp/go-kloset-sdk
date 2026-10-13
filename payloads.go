package sdk

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrDuplicatePayload means the client sent the same payload twice on
// the same stream, which it must not do.
var ErrDuplicatePayload = errors.New("duplicate payload id")

const (
	payloadRead = 1 << iota
	payloadWrite
)

// payload is a record body inflight.  The Export stream sends its
// header and a Put its content; gRPC does not order messages across
// streams, so the two halves can be arrive in either order.
type payload struct {
	rd *io.PipeReader
	wr *io.PipeWriter

	// taken tracks which halves have been handed out.  Once both
	// are, whoever holds them closes them and we stop keeping
	// track.
	taken int
}

// payloads matches the records on the Export stream with the PutData
// streams carrying their bodies.  Its zero value is ready to use, and
// it is good until fail() ends it for good.
type payloads struct {
	mu      sync.Mutex
	pending map[uint64]*payload
	err     error
}

// claim takes one side of the payload.
func (ps *payloads) claim(id uint64, half int) (*payload, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.err != nil {
		return nil, ps.err
	}
	if ps.pending == nil {
		ps.pending = make(map[uint64]*payload)
	}

	pl, ok := ps.pending[id]
	if !ok {
		rd, wr := io.Pipe()
		pl = &payload{rd: rd, wr: wr}
		ps.pending[id] = pl
	}

	if pl.taken&half != 0 {
		return nil, fmt.Errorf("payload %d: %w", id, ErrDuplicatePayload)
	}
	pl.taken |= half

	if pl.taken == payloadRead|payloadWrite {
		delete(ps.pending, id)
	}

	return pl, nil
}

// reader returns the half the exporter reads the body from.
func (ps *payloads) reader(id uint64) (*io.PipeReader, error) {
	pl, err := ps.claim(id, payloadRead)
	if err != nil {
		return nil, err
	}
	return pl.rd, nil
}

// writer returns the half a PutData stream writes the body to.
func (ps *payloads) writer(id uint64) (*io.PipeWriter, error) {
	pl, err := ps.claim(id, payloadWrite)
	if err != nil {
		return nil, err
	}
	return pl.wr, nil
}

// fail terminates the registry: every body still waiting for its
// other half is released with err.  A body both sides already hold is
// untouched.
func (ps *payloads) fail(err error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.err == nil {
		ps.err = err
	}

	for id, pl := range ps.pending {
		// close the untaken side, so that whoever is waiting
		// on it (reader or writer) gets the right error
		// instead of io.ErrClosedPipe.
		if pl.taken&payloadRead != 0 {
			pl.wr.CloseWithError(err)
		} else {
			pl.rd.CloseWithError(err)
		}
		delete(ps.pending, id)
	}
}

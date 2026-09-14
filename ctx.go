package sdk

import (
	"context"
	"os"
	"os/signal"
)

// InterruptableContext wraps the given context with a signal
// interrupt channel, that cancels the returned context on signal
// delivery.  After cancelling the context, the signal handler is
// disarmed, so that subsequent interrupts won't be caught.
func InterruptableContext(ctx context.Context) context.Context {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx
}

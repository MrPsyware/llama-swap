package server

import (
	"net/http"
	"sync/atomic"

	"github.com/mostlygeek/llama-swap/internal/chain"
	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/shared"
)

// globalSemaphore is a channel-based counting semaphore for limiting
// total concurrent requests across all models.
type globalSemaphore chan struct{}

func newGlobalSemaphore(limit int) globalSemaphore {
	return make(chan struct{}, limit)
}

// CreateGlobalConcurrencyMiddleware returns a middleware that blocks incoming
// requests until a semaphore slot is available, enforcing a global cap of
// limit concurrent in-flight requests. limit <= 0 means no cap (pass-through).
func CreateGlobalConcurrencyMiddleware(limit int) chain.Middleware {
	if limit <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	sem := newGlobalSemaphore(limit)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sem <- struct{}{}
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		})
	}
}

// inflightCounter tracks the number of in-flight model-dispatched requests.
type inflightCounter struct {
	total atomic.Int64
}

func (c *inflightCounter) Increment() int64 { return c.total.Add(1) }
func (c *inflightCounter) Decrement() int64 { return c.total.Add(-1) }
func (c *inflightCounter) Current() int64   { return c.total.Load() }

// CreateInflightMiddleware returns middleware that increments the counter on
// entry and decrements on exit, emitting an InFlightRequestsEvent for each.
func CreateInflightMiddleware(c *inflightCounter) chain.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			event.Emit(shared.InFlightRequestsEvent{Total: int(c.Increment())})
			defer func() {
				event.Emit(shared.InFlightRequestsEvent{Total: int(c.Decrement())})
			}()
			next.ServeHTTP(w, r)
		})
	}
}

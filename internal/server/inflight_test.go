package server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGlobalConcurrencyMiddleware_Unlimited(t *testing.T) {
	mw := CreateGlobalConcurrencyMiddleware(0)

	var concurrent atomic.Int32
	var maxSeen atomic.Int32

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := concurrent.Add(1)
		for {
			old := maxSeen.Load()
			if n <= old || maxSeen.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}()
	}
	wg.Wait()

	if maxSeen.Load() < 2 {
		t.Errorf("expected >1 concurrent requests with limit=0, got max=%d", maxSeen.Load())
	}
}

func TestGlobalConcurrencyMiddleware_LimitOne(t *testing.T) {
	mw := CreateGlobalConcurrencyMiddleware(1)

	var concurrent atomic.Int32

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := concurrent.Add(1)
		if n > 1 {
			t.Errorf("concurrent=%d, want <= 1", n)
		}
		time.Sleep(5 * time.Millisecond)
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}()
	}
	wg.Wait()
}

func TestGlobalConcurrencyMiddleware_LimitTwo(t *testing.T) {
	mw := CreateGlobalConcurrencyMiddleware(2)

	var concurrent atomic.Int32
	var violations atomic.Int32

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := concurrent.Add(1)
		if n > 2 {
			violations.Add(1)
		}
		time.Sleep(10 * time.Millisecond)
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}()
	}
	wg.Wait()

	if violations.Load() > 0 {
		t.Errorf("concurrency limit 2 violated %d times", violations.Load())
	}
}

func TestGlobalConcurrencyMiddleware_RequestsAllComplete(t *testing.T) {
	mw := CreateGlobalConcurrencyMiddleware(1)

	var completed atomic.Int32
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		completed.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	const n = 10
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}()
	}
	wg.Wait()

	if completed.Load() != n {
		t.Errorf("completed=%d want %d", completed.Load(), n)
	}
}

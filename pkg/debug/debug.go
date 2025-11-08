package debug

import (
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	readyStateNotReady = 0
	readyStateReady    = 1
)

var (
	readyState atomic.Int64

	// Custom handlers registered by other packages
	customHandlersMu sync.RWMutex
	customHandlers   = make(map[string]http.Handler)

	// Custom readiness check function (optional)
	customReadyCheckMu sync.RWMutex
	customReadyCheck   func() bool

	// Global registry for custom metrics
	globalRegistry = prometheus.NewRegistry()
)

func SetReady() {
	readyState.Store(readyStateReady)
}

func SetNotReady() {
	readyState.Store(readyStateNotReady)
}

// SetReadyCheck registers a custom readiness check function.
// If set, IsReady() will return true only if both:
// 1. SetReady() has been called, AND
// 2. The custom check function returns true
func SetReadyCheck(check func() bool) {
	customReadyCheckMu.Lock()
	defer customReadyCheckMu.Unlock()
	customReadyCheck = check
}

func IsReady() bool {
	if readyState.Load() != readyStateReady {
		return false
	}

	// Check custom readiness function if registered
	customReadyCheckMu.RLock()
	check := customReadyCheck
	customReadyCheckMu.RUnlock()

	if check != nil {
		return check()
	}

	return true
}

// RegisterHandler registers a custom handler on the debug mux.
// Must be called before GetMux() to be included.
func RegisterHandler(pattern string, handler http.Handler) {
	customHandlersMu.Lock()
	defer customHandlersMu.Unlock()
	customHandlers[pattern] = handler
}

// RegisterHandlerFunc registers a custom handler function on the debug mux.
// Must be called before GetMux() to be included.
func RegisterHandlerFunc(pattern string, handler http.HandlerFunc) {
	RegisterHandler(pattern, handler)
}

// Registry returns the Prometheus registry for registering custom metrics.
// Metrics registered here will be exported on /metrics alongside default metrics.
func Registry() prometheus.Registerer {
	return globalRegistry
}

func GetMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Create a gatherer that combines default metrics with our custom registry
	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		globalRegistry,
	}
	mux.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))
	mux.Handle("/debug/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/allocs/", pprof.Handler("allocs"))
	mux.Handle("/debug/block/", pprof.Handler("block"))
	mux.Handle("/debug/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/goroutine/", pprof.Handler("goroutine"))
	mux.Handle("/debug/heap/", pprof.Handler("heap"))
	mux.Handle("/debug/mutex/", pprof.Handler("mutex"))
	mux.Handle("/debug/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/trace", http.HandlerFunc(pprof.Trace))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if IsReady() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	// Register custom handlers
	customHandlersMu.RLock()
	defer customHandlersMu.RUnlock()
	for pattern, handler := range customHandlers {
		mux.Handle(pattern, handler)
	}

	return mux
}

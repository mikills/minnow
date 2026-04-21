package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

const cacheSweepTimeout = 150 * time.Millisecond

// kbIDHeader is the header nginx uses to forward the consistent-hash routing key.
const kbIDHeader = "X-KB-ID"

// kbIDContextKey is the echo context key for the resolved kb id.
const kbIDContextKey = "kb_id"

type AppConfig struct {
	Address               string
	ReadHeaderTimeout     time.Duration
	ShutdownTimeout       time.Duration
	CacheEvictionInterval time.Duration
	MaxMediaBytes         int64
	Logger                *slog.Logger
}

func DefaultAppConfig() AppConfig {
	return AppConfig{
		Address:               "127.0.0.1:8080",
		ReadHeaderTimeout:     5 * time.Second,
		ShutdownTimeout:       5 * time.Second,
		CacheEvictionInterval: 30 * time.Second,
		Logger:                slog.Default(),
	}
}

type App struct {
	kb      *kb.KB
	echo    *echo.Echo
	config  AppConfig
	logger  *slog.Logger
	metrics kb.AppMetrics

	mu       sync.Mutex
	listener net.Listener
	errCh    chan error
	started  bool

	cacheEvictCancel context.CancelFunc
	cacheEvictDone   chan struct{}
}

// Metrics returns the AppMetrics observer for this app.
func (a *App) Metrics() kb.AppMetrics {
	if a == nil {
		return nil
	}
	return a.metrics
}

func NewApp(loader *kb.KB, cfg AppConfig) *App {
	cfg = mergeWithDefaultAppConfig(cfg)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	metrics := kb.AppMetrics(kb.NoopAppMetrics{})
	if m := kb.NewInMemAppMetrics(); m != nil {
		metrics = m
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(requestLoggerMiddleware(logger, metrics))
	e.Use(kbIDMiddleware())

	app := &App{
		kb:      loader,
		echo:    e,
		config:  cfg,
		logger:  logger,
		metrics: metrics,
		errCh:   make(chan error, 1),
	}
	app.registerRoutes()
	return app
}

// kbIDMiddleware extracts the X-KB-ID header set by nginx consistent-hash
// routing and stores it in the echo context for observability. the value
// is also echoed back in the response header for debugging.
func kbIDMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			id := strings.TrimSpace(c.Request().Header.Get(kbIDHeader))
			if id != "" {
				c.Set(kbIDContextKey, id)
				c.Response().Header().Set(kbIDHeader, id)
			}
			return next(c)
		}
	}
}

func mergeWithDefaultAppConfig(cfg AppConfig) AppConfig {
	d := DefaultAppConfig()
	if cfg.Address != "" {
		d.Address = cfg.Address
	}
	if cfg.ReadHeaderTimeout > 0 {
		d.ReadHeaderTimeout = cfg.ReadHeaderTimeout
	}
	if cfg.ShutdownTimeout > 0 {
		d.ShutdownTimeout = cfg.ShutdownTimeout
	}
	if cfg.CacheEvictionInterval > 0 {
		d.CacheEvictionInterval = cfg.CacheEvictionInterval
	}
	if cfg.Logger != nil {
		d.Logger = cfg.Logger
	}
	if cfg.MaxMediaBytes > 0 {
		d.MaxMediaBytes = cfg.MaxMediaBytes
	}
	return d
}

func requestLoggerMiddleware(logger *slog.Logger, metrics kb.AppMetrics) echo.MiddlewareFunc {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = kb.NoopAppMetrics{}
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			if err != nil {
				c.Error(err)
			}

			status := c.Response().Status
			if status == 0 {
				status = http.StatusOK
			}
			latencyMS := time.Since(start).Milliseconds()
			path := c.Path()
			if path == "" {
				path = c.Request().URL.Path
			}
			metrics.RecordRequest(c.Request().Method, path, status, latencyMS)
			attrs := []any{
				"method", c.Request().Method,
				"path", path,
				"status", status,
				"latency_ms", latencyMS,
				"remote_ip", c.RealIP(),
			}

			switch {
			case status >= http.StatusInternalServerError:
				logger.ErrorContext(c.Request().Context(), "http request", attrs...)
			case status >= http.StatusBadRequest:
				logger.WarnContext(c.Request().Context(), "http request", attrs...)
			default:
				logger.InfoContext(c.Request().Context(), "http request", attrs...)
			}
			return nil
		}
	}
}

func (a *App) registerRoutes() {
	deps := Dependencies{
		CacheMetricsHandler: kb.NewCacheOpenMetricsHandler(a.kb),
		SweepCache: func(ctx context.Context) error {
			if a.kb == nil {
				return fmt.Errorf("kb unavailable")
			}
			return a.kb.SweepCache(ctx)
		},
		IsBudgetExceeded: func(err error) bool {
			return errors.Is(err, kb.ErrCacheBudgetExceeded)
		},
		Embed: func(ctx context.Context, input string) ([]float32, error) {
			if a.kb == nil {
				return nil, fmt.Errorf("kb unavailable")
			}
			return a.kb.Embed(ctx, input)
		},
		Search: func(ctx context.Context, kbID string, queryVec []float32, opts *kb.SearchOptions) ([]kb.ExpandedResult, error) {
			if a.kb == nil {
				return nil, fmt.Errorf("kb unavailable")
			}
			return a.kb.Search(ctx, kbID, queryVec, opts)
		},
		MaxMediaBytes: a.config.MaxMediaBytes,
		Logger:        a.logger,
		AppMetrics:    a.metrics,
	}
	// Media route closures are only installed when a MediaStore is wired.
	// When media is disabled in config, KB.MediaStore is nil and these
	// closures stay nil, which makes cmd/actions.go return 503.
	if a.kb != nil && a.kb.MediaStore != nil {
		deps.GetMedia = func(ctx context.Context, id string) (*kb.MediaObject, error) {
			return a.kb.MediaStore.Get(ctx, id)
		}
		deps.ListMedia = func(ctx context.Context, kbID, prefix, after string, limit int) (kb.MediaPage, error) {
			return a.kb.MediaStore.List(ctx, kbID, prefix, after, limit)
		}
		if a.kb.EventStore != nil {
			deps.AppendMediaUpload = func(ctx context.Context, in kb.MediaUploadInput, maxBytes int64, idem, corr string) (string, string, error) {
				return a.kb.AppendMediaUploadDetailed(ctx, in, maxBytes, idem, corr)
			}
		}
	}
	if a.kb != nil && a.kb.EventStore != nil {
		deps.AppendDocumentUpsert = func(ctx context.Context, p kb.DocumentUpsertPayload, idem, corr string) (string, string, error) {
			return a.kb.AppendDocumentUpsertDetailed(ctx, p, idem, corr)
		}
		deps.AppendFileIngest = func(ctx context.Context, in kb.FileIngestInput, maxBytes int64, idem, corr string) (string, string, error) {
			return a.kb.AppendFileIngestDetailed(ctx, in, maxBytes, idem, corr)
		}
		deps.GetEvent = func(ctx context.Context, id string) (*kb.KBEvent, error) {
			return a.kb.EventStore.Get(ctx, id)
		}
		deps.FindOperationTerminal = func(ctx context.Context, source string) (*kb.KBEvent, error) {
			return a.kb.FindOperationTerminal(ctx, source)
		}
		deps.OperationStages = func(ctx context.Context, source string) ([]kb.OperationStageSnapshot, error) {
			return a.kb.OperationStages(ctx, source)
		}
	}
	Register(a.echo, deps)
	RegisterUI(a.echo)
}

func (a *App) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return fmt.Errorf("app already started")
	}

	if a.kb != nil {
		a.startCacheEvictionLoopLocked()
	}

	ln, err := net.Listen("tcp", a.config.Address)
	if err != nil {
		if a.kb != nil {
			a.stopCacheEvictionLoopLocked()
		}
		return err
	}
	a.listener = ln
	a.started = true

	srv := &http.Server{Handler: a.echo, ReadHeaderTimeout: a.config.ReadHeaderTimeout}
	a.echo.Server = srv

	go func() {
		err := a.echo.Server.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		a.errCh <- err
	}()

	return nil
}

func (a *App) Address() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.listener == nil {
		return ""
	}
	addr := a.listener.Addr().String()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "::" || host == "0.0.0.0" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func (a *App) Wait() error {
	return <-a.errCh
}

func (a *App) Stop(ctx context.Context) error {
	a.mu.Lock()
	started := a.started
	a.started = false
	a.mu.Unlock()

	if !started {
		return nil
	}

	if a.kb != nil {
		a.mu.Lock()
		a.stopCacheEvictionLoopLocked()
		a.mu.Unlock()
	}

	if ctx == nil {
		c, cancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
		defer cancel()
		ctx = c
	}

	if err := a.echo.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func (a *App) startCacheEvictionLoopLocked() {
	if a.kb == nil || a.config.CacheEvictionInterval <= 0 {
		return
	}
	if a.cacheEvictCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	a.cacheEvictCancel = cancel
	a.cacheEvictDone = done
	interval := a.config.CacheEvictionInterval

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, sweepCancel := context.WithTimeout(context.Background(), cacheSweepTimeout)
				_ = a.kb.SweepCache(sweepCtx)
				sweepCancel()
			}
		}
	}()
}

func (a *App) stopCacheEvictionLoopLocked() {
	if a.cacheEvictCancel == nil {
		return
	}
	cancel := a.cacheEvictCancel
	done := a.cacheEvictDone
	a.cacheEvictCancel = nil
	a.cacheEvictDone = nil
	cancel()
	if done != nil {
		<-done
	}
}

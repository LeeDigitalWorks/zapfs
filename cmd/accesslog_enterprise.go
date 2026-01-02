//go:build enterprise

package cmd

import (
	"context"
	"time"

	"github.com/LeeDigitalWorks/zapfs/enterprise/accesslog"
	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/api"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// AccessLogConfig configures the access log collector.
type AccessLogConfig struct {
	Enabled        bool
	ClickHouseDSN  string
	BatchSize      int
	FlushInterval  time.Duration
	ExportInterval time.Duration
	DB             db.DB // For reading bucket logging configs
}

// AccessLogManager wraps the access log components.
type AccessLogManager struct {
	store     accesslog.Store
	collector accesslog.Collector
	exporter  *accesslog.S3Exporter
}

// InitializeAccessLog creates and starts the access log collector if licensed.
func InitializeAccessLog(ctx context.Context, cfg AccessLogConfig) (*AccessLogManager, error) {
	if !cfg.Enabled {
		logger.Debug().Msg("access logging disabled")
		return nil, nil
	}

	// Check license
	mgr := license.GetManager()
	if mgr == nil {
		logger.Warn().Msg("access logging not started: no license manager")
		return nil, nil
	}
	if err := mgr.CheckFeature(license.FeatureAuditLog); err != nil {
		logger.Warn().Err(err).Msg("access logging not started: FeatureAuditLog not licensed")
		return nil, nil
	}

	if cfg.ClickHouseDSN == "" {
		logger.Warn().Msg("access logging not started: clickhouse_dsn not configured")
		return nil, nil
	}

	// Build accesslog config
	accessCfg := accesslog.DefaultConfig()
	accessCfg.DSN = cfg.ClickHouseDSN
	if cfg.BatchSize > 0 {
		accessCfg.BatchSize = cfg.BatchSize
	}
	if cfg.FlushInterval > 0 {
		accessCfg.FlushInterval = cfg.FlushInterval
	}
	if cfg.ExportInterval > 0 {
		accessCfg.ExportInterval = cfg.ExportInterval
	}

	// Create ClickHouse store
	store, err := accesslog.NewClickHouseStore(accessCfg)
	if err != nil {
		return nil, err
	}

	// Create collector
	collector := accesslog.NewCollector(accessCfg, store)
	collector.Start(ctx)

	logger.Info().
		Str("dsn", maskDSN(cfg.ClickHouseDSN)).
		Int("batch_size", accessCfg.BatchSize).
		Dur("flush_interval", accessCfg.FlushInterval).
		Dur("export_interval", accessCfg.ExportInterval).
		Msg("access logging enabled")

	return &AccessLogManager{
		store:     store,
		collector: collector,
	}, nil
}

// Collector returns the collector as an api.AccessLogCollector interface.
func (m *AccessLogManager) Collector() api.AccessLogCollector {
	if m == nil || m.collector == nil {
		return nil
	}
	// Wrap the collector to convert event types
	return &accessLogCollectorWrapper{collector: m.collector}
}

// Stop shuts down the access log components.
func (m *AccessLogManager) Stop() {
	if m == nil {
		return
	}
	if m.collector != nil {
		m.collector.Stop()
	}
	if m.exporter != nil {
		m.exporter.Stop()
	}
	if m.store != nil {
		m.store.Close()
	}
	logger.Info().Msg("access logging stopped")
}

// accessLogCollectorWrapper wraps an accesslog.Collector to implement api.AccessLogCollector.
type accessLogCollectorWrapper struct {
	collector accesslog.Collector
}

func (w *accessLogCollectorWrapper) Record(event *api.AccessLogEvent) {
	if w.collector == nil || event == nil {
		return
	}
	// Convert api.AccessLogEvent to accesslog.AccessLogEvent
	w.collector.Record(&accesslog.AccessLogEvent{
		EventTime:        event.EventTime,
		RequestID:        event.RequestID,
		Bucket:           event.Bucket,
		ObjectKey:        event.ObjectKey,
		OwnerID:          event.OwnerID,
		RequesterID:      event.RequesterID,
		RemoteIP:         event.RemoteIP,
		Operation:        event.Operation,
		HTTPMethod:       event.HTTPMethod,
		HTTPStatus:       event.HTTPStatus,
		BytesSent:        event.BytesSent,
		ObjectSize:       event.ObjectSize,
		TotalTimeMs:      event.TotalTimeMs,
		TurnAroundMs:     event.TurnAroundMs,
		SignatureVersion: event.SignatureVersion,
		TLSVersion:       event.TLSVersion,
		AuthType:         event.AuthType,
		UserAgent:        event.UserAgent,
		Referer:          event.Referer,
		HostHeader:       event.HostHeader,
		RequestURI:       event.RequestURI,
		ErrorCode:        event.ErrorCode,
		VersionID:        event.VersionID,
	})
}

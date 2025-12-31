package filter

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3action"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// RateLimitFilter implements request rate limiting and bandwidth throttling
// with support for tiered limits.
//
// This filter provides:
// 1. Operation-specific rate limiting (read/write/list RPS)
// 2. Bandwidth limiting (bytes per second)
// 3. Tiered limits per bucket or user
// 4. Global, per-bucket, per-user, and per-IP limits
//
// The filter uses token bucket algorithm for smooth rate limiting.
type RateLimitFilter struct {
	// Global limits
	globalReadLimiter    *TokenBucket
	globalWriteLimiter   *TokenBucket
	globalListLimiter    *TokenBucket
	globalReadBWLimiter  *TokenBucket
	globalWriteBWLimiter *TokenBucket

	// Per-key limiters (bucket, user, IP)
	bucketLimiters sync.Map // bucket -> *tieredLimiters
	userLimiters   sync.Map // userID -> *tieredLimiters
	ipLimiters     sync.Map // IP -> *limiters

	// Tier configuration
	tierConfig *utils.TierConfig

	config RateLimitConfig
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	// Global limits (0 = unlimited)
	GlobalReadRPS      int64 // Max read requests per second globally
	GlobalWriteRPS     int64 // Max write requests per second globally
	GlobalListRPS      int64 // Max list requests per second globally
	GlobalReadBytesPS  int64 // Max read bytes per second globally
	GlobalWriteBytesPS int64 // Max write bytes per second globally

	// Per-IP limits (useful for anonymous/unauthenticated requests)
	IPReadRPS     int64
	IPWriteRPS    int64
	IPListRPS     int64
	IPBytesPerSec int64

	// Burst multiplier - allows temporary bursts above the rate limit
	// e.g., BurstMultiplier=2 allows 2x the rate limit temporarily
	BurstMultiplier int64

	// CleanupInterval for removing stale per-key limiters
	CleanupInterval time.Duration
}

// tieredLimiters holds rate and bandwidth limiters for a single entity with tier support
type tieredLimiters struct {
	readRPS  *TokenBucket
	writeRPS *TokenBucket
	listRPS  *TokenBucket
	readBW   *TokenBucket
	writeBW  *TokenBucket
	tier     string       // Current tier name
	lastUsed atomic.Int64 // Unix timestamp
}

// limiters holds simple rate and bandwidth limiters (for IP limiting)
type limiters struct {
	readRPS   *TokenBucket
	writeRPS  *TokenBucket
	listRPS   *TokenBucket
	bandwidth *TokenBucket
	lastUsed  atomic.Int64
}

// DefaultRateLimitConfig returns sensible defaults
// Uses 800/800/100 for read/write/list RPS and 1GB/s bandwidth
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		// Global limits - generous, mainly for protection
		GlobalReadRPS:      50000,    // 50K read RPS global
		GlobalWriteRPS:     30000,    // 30K write RPS global
		GlobalListRPS:      5000,     // 5K list RPS global
		GlobalReadBytesPS:  10 << 30, // 10 GB/s global read
		GlobalWriteBytesPS: 10 << 30, // 10 GB/s global write

		// Per-IP limits (for anonymous requests)
		IPReadRPS:     100,
		IPWriteRPS:    50,
		IPListRPS:     10,
		IPBytesPerSec: 10 << 20, // 10 MB/s per IP

		BurstMultiplier: 2,
		CleanupInterval: 5 * time.Minute,
	}
}

// TierStore interface for looking up entity tiers
type TierStore interface {
	// GetBucketTier returns the tier name for a bucket
	GetBucketTier(bucket string) string
	// GetUserTier returns the tier name for a user
	GetUserTier(userID string) string
}

func NewRateLimitFilter(config RateLimitConfig, tierConfig *utils.TierConfig) *RateLimitFilter {
	if tierConfig == nil {
		tierConfig = utils.LoadTierConfig()
	}

	f := &RateLimitFilter{
		config:     config,
		tierConfig: tierConfig,
	}

	burst := config.BurstMultiplier
	if burst < 1 {
		burst = 1
	}

	// Initialize global limiters
	if config.GlobalReadRPS > 0 {
		f.globalReadLimiter = NewTokenBucket(config.GlobalReadRPS, config.GlobalReadRPS*burst)
	}
	if config.GlobalWriteRPS > 0 {
		f.globalWriteLimiter = NewTokenBucket(config.GlobalWriteRPS, config.GlobalWriteRPS*burst)
	}
	if config.GlobalListRPS > 0 {
		f.globalListLimiter = NewTokenBucket(config.GlobalListRPS, config.GlobalListRPS*burst)
	}
	if config.GlobalReadBytesPS > 0 {
		f.globalReadBWLimiter = NewTokenBucket(config.GlobalReadBytesPS, config.GlobalReadBytesPS*burst)
	}
	if config.GlobalWriteBytesPS > 0 {
		f.globalWriteBWLimiter = NewTokenBucket(config.GlobalWriteBytesPS, config.GlobalWriteBytesPS*burst)
	}

	// Start cleanup goroutine
	if config.CleanupInterval > 0 {
		go f.cleanupLoop()
	}

	return f
}

func (f *RateLimitFilter) Type() string {
	return "rate_limit"
}

func (f *RateLimitFilter) Run(d *data.Data) (Response, error) {
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	// Determine operation type from the S3 action
	opType := d.S3Info.Action.OperationType()

	// Extract request metadata
	bucket := d.S3Info.Bucket
	clientIP := getClientIP(d.Req)

	// Estimate request size for bandwidth limiting
	contentLength := d.Req.ContentLength
	if contentLength < 0 {
		contentLength = 0
	}

	// Check global rate limits by operation type
	if err := f.checkGlobalLimits(opType, contentLength); err != nil {
		return nil, err
	}

	// Check per-bucket limits (tiered)
	if bucket != "" {
		tier := f.getBucketTier(bucket)
		if err := f.checkBucketLimits(bucket, tier, opType, contentLength); err != nil {
			return nil, err
		}
	}

	// Check per-user limits (tiered, for authenticated requests)
	var userID string
	if d.Identity != nil {
		if d.Identity.Account != nil {
			userID = d.Identity.Account.ID
		} else {
			userID = d.Identity.Name
		}
	}
	if userID != "" {
		tier := f.getUserTier(userID)
		if err := f.checkUserLimits(userID, tier, opType, contentLength); err != nil {
			return nil, err
		}
	}

	// Check per-IP limits (especially important for anonymous requests)
	if clientIP != "" {
		if err := f.checkIPLimits(clientIP, opType, contentLength); err != nil {
			return nil, err
		}
	}

	return Next{}, nil
}

// checkGlobalLimits checks global rate and bandwidth limits
func (f *RateLimitFilter) checkGlobalLimits(opType s3action.OperationType, contentLength int64) error {
	// Check operation-specific rate limit
	switch opType {
	case s3action.OpRead:
		if f.globalReadLimiter != nil && !f.globalReadLimiter.Take(1) {
			return s3err.ErrSlowDown
		}
	case s3action.OpWrite:
		if f.globalWriteLimiter != nil && !f.globalWriteLimiter.Take(1) {
			return s3err.ErrSlowDown
		}
		// Check write bandwidth for uploads
		if f.globalWriteBWLimiter != nil && contentLength > 0 {
			if !f.globalWriteBWLimiter.Take(contentLength) {
				return s3err.ErrSlowDown
			}
		}
	case s3action.OpList:
		if f.globalListLimiter != nil && !f.globalListLimiter.Take(1) {
			return s3err.ErrSlowDown
		}
	}

	return nil
}

// getBucketTier returns the tier for a bucket
func (f *RateLimitFilter) getBucketTier(bucket string) string {
	// Check collection_tiers overrides first
	if tier, ok := f.tierConfig.CollectionTiers[bucket]; ok {
		return tier
	}
	return f.tierConfig.DefaultTier
}

// getUserTier returns the tier for a user
// TODO: This should look up user tier from IAM or billing system
func (f *RateLimitFilter) getUserTier(userID string) string {
	// For now, use default tier for all users
	// In production, this would query the IAM system or billing database
	return f.tierConfig.DefaultTier
}

// getTierLimits returns the limits for a tier name
func (f *RateLimitFilter) getTierLimits(tierName string) utils.RateLimitTier {
	if tier, ok := f.tierConfig.Tiers[tierName]; ok {
		return tier
	}
	// Fall back to default tier
	if tier, ok := f.tierConfig.Tiers[f.tierConfig.DefaultTier]; ok {
		return tier
	}
	// Ultimate fallback to hardcoded defaults
	return utils.RateLimitTier{
		Name:              "default",
		MaxReadRPS:        800,
		MaxWriteRPS:       800,
		MaxListRPS:        100,
		MaxReadBandwidth:  1 << 30, // 1 GB/s
		MaxWriteBandwidth: 1 << 30, // 1 GB/s
	}
}

// checkBucketLimits checks per-bucket tiered limits
func (f *RateLimitFilter) checkBucketLimits(bucket, tier string, opType s3action.OperationType, contentLength int64) error {
	tl := f.getOrCreateTieredLimiters(&f.bucketLimiters, bucket, tier)

	switch opType {
	case s3action.OpRead:
		if tl.readRPS != nil && !tl.readRPS.Take(1) {
			return s3err.ErrSlowDown
		}
	case s3action.OpWrite:
		if tl.writeRPS != nil && !tl.writeRPS.Take(1) {
			return s3err.ErrSlowDown
		}
		if tl.writeBW != nil && contentLength > 0 && !tl.writeBW.Take(contentLength) {
			return s3err.ErrSlowDown
		}
	case s3action.OpList:
		if tl.listRPS != nil && !tl.listRPS.Take(1) {
			return s3err.ErrSlowDown
		}
	}

	return nil
}

// checkUserLimits checks per-user tiered limits
func (f *RateLimitFilter) checkUserLimits(userID, tier string, opType s3action.OperationType, contentLength int64) error {
	tl := f.getOrCreateTieredLimiters(&f.userLimiters, userID, tier)

	switch opType {
	case s3action.OpRead:
		if tl.readRPS != nil && !tl.readRPS.Take(1) {
			return s3err.ErrTooManyRequests
		}
	case s3action.OpWrite:
		if tl.writeRPS != nil && !tl.writeRPS.Take(1) {
			return s3err.ErrTooManyRequests
		}
		if tl.writeBW != nil && contentLength > 0 && !tl.writeBW.Take(contentLength) {
			return s3err.ErrRequestBytesExceed
		}
	case s3action.OpList:
		if tl.listRPS != nil && !tl.listRPS.Take(1) {
			return s3err.ErrTooManyRequests
		}
	}

	return nil
}

// checkIPLimits checks per-IP limits
func (f *RateLimitFilter) checkIPLimits(clientIP string, opType s3action.OperationType, contentLength int64) error {
	il := f.getOrCreateIPLimiters(clientIP)

	switch opType {
	case s3action.OpRead:
		if il.readRPS != nil && !il.readRPS.Take(1) {
			return s3err.ErrTooManyRequests
		}
	case s3action.OpWrite:
		if il.writeRPS != nil && !il.writeRPS.Take(1) {
			return s3err.ErrTooManyRequests
		}
	case s3action.OpList:
		if il.listRPS != nil && !il.listRPS.Take(1) {
			return s3err.ErrTooManyRequests
		}
	}

	// Bandwidth limit applies to all operation types for IP
	if il.bandwidth != nil && contentLength > 0 && !il.bandwidth.Take(contentLength) {
		return s3err.ErrRequestBytesExceed
	}

	return nil
}

// getOrCreateTieredLimiters gets or creates tiered limiters for a key
func (f *RateLimitFilter) getOrCreateTieredLimiters(m *sync.Map, key, tierName string) *tieredLimiters {
	if v, ok := m.Load(key); ok {
		tl := v.(*tieredLimiters)
		tl.lastUsed.Store(time.Now().Unix())

		// Check if tier changed
		if tl.tier != tierName {
			// Tier changed, recreate limiters
			m.Delete(key)
		} else {
			return tl
		}
	}

	tier := f.getTierLimits(tierName)
	burst := f.config.BurstMultiplier
	if burst < 1 {
		burst = 1
	}

	tl := &tieredLimiters{
		tier: tierName,
	}

	if tier.MaxReadRPS > 0 {
		tl.readRPS = NewTokenBucket(int64(tier.MaxReadRPS), int64(tier.MaxReadRPS)*burst)
	}
	if tier.MaxWriteRPS > 0 {
		tl.writeRPS = NewTokenBucket(int64(tier.MaxWriteRPS), int64(tier.MaxWriteRPS)*burst)
	}
	if tier.MaxListRPS > 0 {
		tl.listRPS = NewTokenBucket(int64(tier.MaxListRPS), int64(tier.MaxListRPS)*burst)
	}
	if tier.MaxReadBandwidth > 0 {
		tl.readBW = NewTokenBucket(tier.MaxReadBandwidth, tier.MaxReadBandwidth*burst)
	}
	if tier.MaxWriteBandwidth > 0 {
		tl.writeBW = NewTokenBucket(tier.MaxWriteBandwidth, tier.MaxWriteBandwidth*burst)
	}

	tl.lastUsed.Store(time.Now().Unix())

	actual, _ := m.LoadOrStore(key, tl)
	return actual.(*tieredLimiters)
}

// getOrCreateIPLimiters gets or creates simple limiters for an IP
func (f *RateLimitFilter) getOrCreateIPLimiters(ip string) *limiters {
	if v, ok := f.ipLimiters.Load(ip); ok {
		l := v.(*limiters)
		l.lastUsed.Store(time.Now().Unix())
		return l
	}

	burst := f.config.BurstMultiplier
	if burst < 1 {
		burst = 1
	}

	l := &limiters{}
	if f.config.IPReadRPS > 0 {
		l.readRPS = NewTokenBucket(f.config.IPReadRPS, f.config.IPReadRPS*burst)
	}
	if f.config.IPWriteRPS > 0 {
		l.writeRPS = NewTokenBucket(f.config.IPWriteRPS, f.config.IPWriteRPS*burst)
	}
	if f.config.IPListRPS > 0 {
		l.listRPS = NewTokenBucket(f.config.IPListRPS, f.config.IPListRPS*burst)
	}
	if f.config.IPBytesPerSec > 0 {
		l.bandwidth = NewTokenBucket(f.config.IPBytesPerSec, f.config.IPBytesPerSec*burst)
	}

	l.lastUsed.Store(time.Now().Unix())

	actual, _ := f.ipLimiters.LoadOrStore(ip, l)
	return actual.(*limiters)
}

// cleanupLoop periodically removes stale limiters
func (f *RateLimitFilter) cleanupLoop() {
	ticker := time.NewTicker(f.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-f.config.CleanupInterval * 2).Unix()
		f.cleanupTieredMap(&f.bucketLimiters, cutoff)
		f.cleanupTieredMap(&f.userLimiters, cutoff)
		f.cleanupIPMap(cutoff)
	}
}

func (f *RateLimitFilter) cleanupTieredMap(m *sync.Map, cutoff int64) {
	m.Range(func(key, value any) bool {
		tl := value.(*tieredLimiters)
		if tl.lastUsed.Load() < cutoff {
			m.Delete(key)
		}
		return true
	})
}

func (f *RateLimitFilter) cleanupIPMap(cutoff int64) {
	f.ipLimiters.Range(func(key, value any) bool {
		l := value.(*limiters)
		if l.lastUsed.Load() < cutoff {
			f.ipLimiters.Delete(key)
		}
		return true
	})
}

// getClientIP extracts client IP from request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For first (may contain multiple IPs, take first)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check X-Real-IP (set by some proxies)
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fall back to RemoteAddr
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// TokenBucket implements a token bucket rate limiter
type TokenBucket struct {
	rate     int64        // Tokens added per second
	capacity int64        // Maximum tokens (burst size)
	tokens   atomic.Int64 // Current tokens (scaled by 1000 for precision)
	lastTime atomic.Int64 // Last refill time (Unix nano)
	mu       sync.Mutex   // For refill operations
}

const tokenScale = 1000 // Scale factor for sub-token precision

// NewTokenBucket creates a new token bucket
func NewTokenBucket(rate, capacity int64) *TokenBucket {
	tb := &TokenBucket{
		rate:     rate,
		capacity: capacity,
	}
	tb.tokens.Store(capacity * tokenScale) // Start full
	tb.lastTime.Store(time.Now().UnixNano())
	return tb
}

// Take attempts to take n tokens, returns true if successful
func (tb *TokenBucket) Take(n int64) bool {
	tb.refill()

	needed := n * tokenScale
	for {
		current := tb.tokens.Load()
		if current < needed {
			return false
		}
		if tb.tokens.CompareAndSwap(current, current-needed) {
			return true
		}
	}
}

// refill adds tokens based on elapsed time
func (tb *TokenBucket) refill() {
	now := time.Now().UnixNano()
	last := tb.lastTime.Load()
	elapsed := now - last

	if elapsed < int64(time.Millisecond) {
		return // Too soon to refill
	}

	// Calculate tokens to add
	tokensToAdd := (elapsed * tb.rate * tokenScale) / int64(time.Second)
	if tokensToAdd <= 0 {
		return
	}

	// Try to update atomically
	if !tb.lastTime.CompareAndSwap(last, now) {
		return // Another goroutine is refilling
	}

	// Add tokens up to capacity
	maxTokens := tb.capacity * tokenScale
	for {
		current := tb.tokens.Load()
		newTokens := current + tokensToAdd
		if newTokens > maxTokens {
			newTokens = maxTokens
		}
		if tb.tokens.CompareAndSwap(current, newTokens) {
			return
		}
	}
}

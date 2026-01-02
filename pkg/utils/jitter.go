package utils

import (
	"math/rand/v2"
	"time"
)

// Jitter adds random jitter to a duration to prevent thundering herd.
// The jitter is applied as a percentage of the base duration.
//
// Example: Jitter(time.Minute, 0.1) returns 54s-66s (±10%)
func Jitter(base time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return base
	}
	if fraction > 1 {
		fraction = 1
	}
	// Generate jitter in range [-fraction, +fraction]
	jitterRange := float64(base) * fraction
	jitter := (rand.Float64()*2 - 1) * jitterRange
	return base + time.Duration(jitter)
}

// JitterUp adds random jitter that only increases the duration.
// Useful when you want minimum spacing but allow longer delays.
//
// Example: JitterUp(time.Minute, 0.25) returns 60s-75s (+0-25%)
func JitterUp(base time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return base
	}
	jitter := rand.Float64() * float64(base) * fraction
	return base + time.Duration(jitter)
}

// JitteredTicker returns a channel that sends at jittered intervals.
// Each tick has independent jitter applied.
// The returned stop function must be called to clean up resources.
func JitteredTicker(base time.Duration, fraction float64) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	done := make(chan struct{})

	go func() {
		for {
			interval := Jitter(base, fraction)
			timer := time.NewTimer(interval)
			select {
			case t := <-timer.C:
				select {
				case ch <- t:
				default:
					// Drop if receiver is slow
				}
			case <-done:
				timer.Stop()
				close(ch)
				return
			}
		}
	}()

	return ch, func() { close(done) }
}

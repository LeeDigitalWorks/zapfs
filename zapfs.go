package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"zapfs/cmd"

	"github.com/getsentry/sentry-go"
)

func main() {
	err := sentry.Init(sentry.ClientOptions{
		SampleRate:       0.1,
		EnableTracing:    true,
		TracesSampleRate: 0.1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "sentry.Init: %v", err)
	}
	// Flush buffered events before the program terminates.
	// Set the timeout to the maximum duration the program can afford to wait.
	defer sentry.Flush(2 * time.Second)

	flag.Parse()

	cmd.Execute()
}

package taskqueue

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
)

// recordQueueDepth updates queue depth metrics from stats
func recordQueueDepth(stats *QueueStats) {
	if stats == nil {
		return
	}
	QueueDepth.WithLabelValues("pending").Set(float64(stats.Pending))
	QueueDepth.WithLabelValues("running").Set(float64(stats.Running))
	QueueDepth.WithLabelValues("failed").Set(float64(stats.Failed))
}

// Worker polls the queue and executes tasks.
type Worker struct {
	id       string
	queue    Queue
	handlers map[TaskType]Handler

	pollInterval time.Duration
	concurrency  int

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// WorkerConfig configures the task worker.
type WorkerConfig struct {
	ID           string
	Queue        Queue
	PollInterval time.Duration
	Concurrency  int
}

// NewWorker creates a new task worker.
func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 5
	}

	return &Worker{
		id:           cfg.ID,
		queue:        cfg.Queue,
		handlers:     make(map[TaskType]Handler),
		pollInterval: cfg.PollInterval,
		concurrency:  cfg.Concurrency,
		stopCh:       make(chan struct{}),
	}
}

// RegisterHandler registers a handler for a task type.
func (w *Worker) RegisterHandler(h Handler) {
	if h == nil {
		return
	}
	w.handlers[h.Type()] = h
	logger.Debug().
		Str("type", string(h.Type())).
		Msg("taskqueue: registered handler")
}

// Start begins processing tasks.
func (w *Worker) Start(ctx context.Context) {
	// Get list of task types we can handle
	types := make([]TaskType, 0, len(w.handlers))
	for t := range w.handlers {
		types = append(types, t)
	}

	if len(types) == 0 {
		logger.Warn().Msg("taskqueue: worker started with no handlers")
		return
	}

	logger.Info().
		Str("worker_id", w.id).
		Int("concurrency", w.concurrency).
		Int("handlers", len(types)).
		Msg("taskqueue: worker starting")

	// Start worker goroutines
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.work(ctx, types)
	}
}

// Stop gracefully shuts down the worker.
func (w *Worker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	logger.Info().Str("worker_id", w.id).Msg("taskqueue: worker stopped")
}

func (w *Worker) work(ctx context.Context, types []TaskType) {
	defer w.wg.Done()

	// Use jittered polling to prevent thundering herd
	// Jitter is up to 25% of the poll interval
	jitterMax := w.pollInterval / 4

	for {
		// Calculate next poll time with jitter
		jitter := time.Duration(rand.Int63n(int64(jitterMax)))
		timer := time.NewTimer(w.pollInterval + jitter)

		select {
		case <-w.stopCh:
			timer.Stop()
			return
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			w.processOne(ctx, types)
		}
	}
}

func (w *Worker) processOne(ctx context.Context, types []TaskType) {
	task, err := w.queue.Dequeue(ctx, w.id, types...)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logger.Error().Err(err).Msg("taskqueue: dequeue failed")
			DequeueErrors.Inc()
		}
		return
	}
	if task == nil {
		// Periodically update queue depth metrics even when idle
		if stats, err := w.queue.Stats(ctx); err == nil {
			recordQueueDepth(stats)
		}
		return
	}

	handler, ok := w.handlers[task.Type]
	if !ok {
		logger.Error().
			Str("task_id", task.ID).
			Str("type", string(task.Type)).
			Msg("taskqueue: no handler for task type")
		w.queue.Fail(ctx, task.ID, errors.New("no handler registered"))
		TasksProcessedTotal.WithLabelValues(string(task.Type), "no_handler").Inc()
		return
	}

	logger.Debug().
		Str("task_id", task.ID).
		Str("type", string(task.Type)).
		Int("attempt", task.Attempts).
		Msg("taskqueue: processing task")

	// Track retries
	if task.Attempts > 1 {
		TaskRetries.WithLabelValues(string(task.Type)).Inc()
	}

	// Process task with timing
	startTime := time.Now()
	handleErr := handler.Handle(ctx, task)
	duration := time.Since(startTime).Seconds()

	TaskProcessingDuration.WithLabelValues(string(task.Type)).Observe(duration)

	if handleErr != nil {
		logger.Warn().
			Err(handleErr).
			Str("task_id", task.ID).
			Str("type", string(task.Type)).
			Int("attempt", task.Attempts).
			Msg("taskqueue: task failed")
		w.queue.Fail(ctx, task.ID, handleErr)
		TasksProcessedTotal.WithLabelValues(string(task.Type), "failed").Inc()
	} else {
		logger.Debug().
			Str("task_id", task.ID).
			Str("type", string(task.Type)).
			Msg("taskqueue: task completed")
		w.queue.Complete(ctx, task.ID)
		TasksProcessedTotal.WithLabelValues(string(task.Type), "completed").Inc()
	}

	// Update queue depth after processing
	if stats, err := w.queue.Stats(ctx); err == nil {
		recordQueueDepth(stats)
	}
}

// Queue returns the underlying queue (for testing/metrics).
func (w *Worker) Queue() Queue {
	return w.queue
}

// HandlerTypes returns the task types this worker handles.
func (w *Worker) HandlerTypes() []TaskType {
	types := make([]TaskType, 0, len(w.handlers))
	for t := range w.handlers {
		types = append(types, t)
	}
	return types
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	defaultWorkerGroup = "ax-controllers"
	// readRetryDelay is how long the worker waits after a transient error from the
	// event queue before trying again.
	readRetryDelay = time.Second
	// Bound status-only retries so a storage failure cannot stall this worker indefinitely.
	statusWriteAttempts   = 3
	statusWriteRetryDelay = 500 * time.Millisecond
)

var errTaskStatusNotPersisted = errors.New("task status not persisted")

// Worker consumes task events from the store's event queue and reconciles each
// task against Substrate. Run several with the same group name to share the load;
// every event is handled by exactly one of them.
type Worker struct {
	store      store.Store
	reconciler *TaskReconciler
	group      string
	consumer   string
}

// NewWorker creates a worker that joins group as consumer. An empty group uses the
// default controller group; an empty consumer derives a unique name from the host.
func NewWorker(s store.Store, reconciler *TaskReconciler, group, consumer string) *Worker {
	if group == "" {
		group = defaultWorkerGroup
	}
	if consumer == "" {
		hostname, _ := os.Hostname()
		consumer = fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%10000)
	}
	return &Worker{
		store:      s,
		reconciler: reconciler,
		group:      group,
		consumer:   consumer,
	}
}

// Run subscribes to task events and processes them until ctx is done. It returns
// ctx.Err() on shutdown. Reconciliation failures are acknowledged so a bad task
// cannot wedge the queue. A successful reconciliation whose status cannot be
// persisted is left unacknowledged after bounded status-only retries.
func (w *Worker) Run(ctx context.Context) error {
	slog.Info("starting AX task worker", "group", w.group, "consumer", w.consumer)

	sub, err := w.store.Subscribe(ctx, w.group, w.consumer)
	if err != nil {
		return fmt.Errorf("subscribing to task events: %w", err)
	}
	defer sub.Close()

	for {
		ev, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("stopping AX task worker")
				return ctx.Err()
			}
			slog.Error("error reading task events", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(readRetryDelay):
			}
			continue
		}

		if err := w.processEvent(ctx, ev); err != nil {
			slog.Error("error processing task event",
				"id", ev.ID,
				"atespace", ev.Atespace,
				"name", ev.Name,
				"action", ev.Action,
				"error", err,
			)
			if errors.Is(err, errTaskStatusNotPersisted) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
		}
		if err := sub.Ack(ctx, ev); err != nil {
			slog.Warn("failed to acknowledge task event", "id", ev.ID, "error", err)
		}
	}
}

func (w *Worker) processEvent(ctx context.Context, ev store.TaskEvent) error {
	if ev.Action == "delete" {
		slog.Info("handling task deletion event", "atespace", ev.Atespace, "name", ev.Name)
		if err := w.reconciler.ReconcileDelete(ctx, ev.Atespace, ev.Name); err != nil {
			// Leave the record in Terminating so the failure is visible; re-running
			// `ax delete` republishes the event and retries the cleanup.
			return fmt.Errorf("cleaning up task %s/%s: %w", ev.Atespace, ev.Name, err)
		}
		if err := w.store.DeleteTask(ctx, ev.Atespace, ev.Name); err != nil {
			return fmt.Errorf("removing task record %s/%s: %w", ev.Atespace, ev.Name, err)
		}
		return nil
	}

	task, err := w.store.GetTask(ctx, ev.Atespace, ev.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			slog.Info("task not found, skipping reconcile", "atespace", ev.Atespace, "name", ev.Name)
			return nil
		}
		return fmt.Errorf("fetching task %s/%s: %w", ev.Atespace, ev.Name, err)
	}
	// A pending delete event owns this task now; reconciling would resume an actor
	// that is about to be torn down and overwrite the Terminating phase.
	if task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
		slog.Info("task is terminating, skipping reconcile", "atespace", ev.Atespace, "name", ev.Name)
		return nil
	}

	// Resolve every bound workspace. A missing one is skipped so the task still
	// runs; the runner creates an empty directory at its path.
	var workspaces []*v1alpha1.Workspace
	for _, ref := range task.Spec.WorkspaceRefs() {
		if ref.Name == "" {
			continue
		}
		wsp, err := w.store.GetWorkspace(ctx, task.Metadata.Atespace, ref.Name)
		if err == nil {
			workspaces = append(workspaces, wsp)
		} else if !errors.Is(err, store.ErrNotFound) {
			slog.Warn("error fetching workspace", "name", ref.Name, "error", err)
		}
	}

	reconciled, err := w.reconciler.Reconcile(ctx, task, workspaces...)
	if err != nil {
		task.Status.Phase = "Failed"
		_ = w.store.UpdateTaskStatus(ctx, task.Metadata.Atespace, task.Metadata.Name, task.Status)
		return fmt.Errorf("reconciling task %s/%s: %w", task.Metadata.Atespace, task.Metadata.Name, err)
	}

	if err := w.persistTaskStatus(ctx, reconciled); err != nil {
		return fmt.Errorf("%w for %s/%s: %w", errTaskStatusNotPersisted, task.Metadata.Atespace, task.Metadata.Name, err)
	}

	return nil
}

// persistTaskStatus retries only the computed status, never the Substrate effects
// of the successful reconciliation. Exhaustion leaves the event unacknowledged;
// this worker does not replay pending events.
func (w *Worker) persistTaskStatus(ctx context.Context, task *v1alpha1.Task) error {
	var err error
	for attempt := 1; attempt <= statusWriteAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err = w.store.UpdateTaskStatus(ctx, task.Metadata.Atespace, task.Metadata.Name, task.Status)
		if err == nil {
			return nil
		}
		if attempt == statusWriteAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(statusWriteRetryDelay):
		}
	}
	return err
}

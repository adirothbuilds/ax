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

package controller_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestWorkerReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start in-process mock Substrate server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// 2. Substrate client
	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	// 3. In-memory store
	memStore := memory.NewStore()

	// Save task
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "worker-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "ghrc.io/test/img",
		},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}

	// 4. Start the worker in the background
	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	go func() {
		_ = worker.Run(ctx)
	}()

	// 5. Poll store until task reaches "Running" phase
	deadline := time.Now().Add(3 * time.Second)
	var finalTask *v1alpha1.Task
	for time.Now().Before(deadline) {
		tItem, err := memStore.GetTask(ctx, "default", "worker-task")
		if err == nil && tItem.Status.Phase == "Running" {
			finalTask = tItem
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalTask == nil {
		t.Fatalf("task did not transition to Running phase in time")
	}

	if finalTask.Status.Actor != "worker-task" {
		t.Errorf("expected actor 'worker-task', got %q", finalTask.Status.Actor)
	}
	if finalTask.Status.WorkerIp != "10.244.1.42" {
		t.Errorf("expected worker IP '10.244.1.42', got %q", finalTask.Status.WorkerIp)
	}
}

func TestWorkerStatusPersistence(t *testing.T) {
	for _, tc := range []struct {
		name               string
		failures           int
		reconcileFails     bool
		cancelAfterFailure bool
		steps              []string
		writes, acks       int
		persisted          bool
	}{
		{
			name: "transient failure", failures: 1, writes: 2, acks: 1, persisted: true,
			steps: []string{"delivered", "status write failed", "status written", "ack", "next read"},
		},
		{
			name: "last attempt succeeds", failures: 2, writes: 3, acks: 1, persisted: true,
			steps: []string{"delivered", "status write failed", "status write failed", "status written", "ack", "next read"},
		},
		{
			name: "retries exhausted", failures: 3, writes: 3,
			steps: []string{"delivered", "status write failed", "status write failed", "status write failed", "next read"},
		},
		{
			name: "shutdown after failure", failures: 1, writes: 1, cancelAfterFailure: true,
			steps: []string{"delivered", "status write failed"},
		},
		{
			name: "reconciliation failure is acknowledged", reconcileFails: true, writes: 1, acks: 1, persisted: true,
			steps: []string{"delivered", "status written", "ack", "next read"},
		},
		{
			name: "reconciliation failure keeps existing best effort status policy", reconcileFails: true, failures: 1, writes: 1, acks: 1,
			steps: []string{"delivered", "status write failed", "ack", "next read"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}
			defer lis.Close()
			mockSrv := &statusFailureControlServer{mockControlServer: &mockControlServer{}}
			if tc.reconcileFails {
				mockSrv.createErr = status.Error(codes.InvalidArgument, "invalid actor")
			}
			grpcServer := grpc.NewServer()
			ateapipb.RegisterControlServer(grpcServer, mockSrv)
			go grpcServer.Serve(lis)
			defer grpcServer.Stop()

			subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("failed to create substrate client: %v", err)
			}
			defer subClient.Close()
			reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
			reconciler.SecretResolver = noSecrets

			memStore := memory.NewStore()
			task := &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Name: "status-failure", Atespace: "default"},
				// New Tasks start Suspended; reconciliation records their actor and
				// readiness condition without polling a running workspace.
				Spec:   &v1alpha1.TaskSpec{Image: "ghcr.io/test/img"},
				Status: &v1alpha1.TaskStatus{Phase: "Suspended", Id: "task-id"},
			}
			if err := memStore.SaveTask(ctx, task); err != nil {
				t.Fatalf("SaveTask: %v", err)
			}
			before := proto.Clone(task.Status).(*v1alpha1.TaskStatus)
			injectedErr := errors.New("injected status write failure")
			sub := &statusFailureSubscription{steps: make(chan string)}
			s := &statusFailureStore{Store: memStore, failure: injectedErr, failures: tc.failures, sub: sub}
			if tc.cancelAfterFailure {
				s.cancelOnFailure = cancel
			}
			worker := controller.NewWorker(s, reconciler, "test-group", "worker-1")
			done := make(chan struct{})
			var runErr error
			go func() {
				runErr = worker.Run(ctx)
				close(done)
			}()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop")
				}
			}()

			// Barriers verify that status persistence finishes before ACK or the next
			// read. Timeouts only guard against hangs; no sleeps establish behavior.
			for _, want := range tc.steps {
				select {
				case got := <-sub.steps:
					if got != want {
						t.Fatalf("worker step = %q, want %q", got, want)
					}
				case <-done:
					t.Fatalf("worker stopped while waiting for %q: %v", want, runErr)
				case <-ctx.Done():
					t.Fatalf("waiting for %q: %v", want, ctx.Err())
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not stop after next queue read")
			}
			grpcServer.Stop()
			if !errors.Is(runErr, context.Canceled) {
				t.Errorf("Worker.Run error = %v, want context.Canceled", runErr)
			}
			if len(s.statuses) != tc.writes {
				t.Fatalf("status writes = %d, want %d", len(s.statuses), tc.writes)
			}
			if tc.persisted && s.writeErr != nil || !tc.persisted && !errors.Is(s.writeErr, injectedErr) {
				t.Errorf("last status write error = %v, persisted = %v", s.writeErr, tc.persisted)
			}
			computed := s.statuses[0]
			wantPhase, wantReason := "Suspended", "TaskSuspended"
			if tc.reconcileFails {
				wantPhase, wantReason = "Failed", "ActorCreationFailed"
			}
			if computed.Phase != wantPhase || computed.Actor != task.Metadata.Name || computed.Id != before.Id || computed.WorkerIp != "" {
				t.Errorf("unexpected reconciled status: %v", computed)
			}
			assertCondition(t, &v1alpha1.Task{Status: computed}, "Ready", "False", wantReason)
			for _, attempted := range s.statuses[1:] {
				if !proto.Equal(attempted, computed) {
					t.Errorf("retry changed computed status: got %v, want %v", attempted, computed)
				}
			}
			wantSuspends := 1
			if tc.reconcileFails {
				wantSuspends = 0
			}
			if mockSrv.createCalls != 1 || len(mockSrv.suspendedActors) != wantSuspends {
				t.Errorf("create calls = %d, suspended actors = %v; want one reconciliation and %d suspensions", mockSrv.createCalls, mockSrv.suspendedActors, wantSuspends)
			}
			wantReads := 2
			if tc.cancelAfterFailure {
				wantReads = 1
			}
			if sub.reads != wantReads || len(sub.delivered) != 1 || sub.acks != tc.acks {
				t.Fatalf("reads = %d, deliveries = %d, ACKs = %d; want %d, 1, %d", sub.reads, len(sub.delivered), sub.acks, wantReads, tc.acks)
			}
			if tc.acks != 0 && (sub.ackErr != nil || sub.acked != sub.delivered[0] || sub.acked.ID == "") {
				t.Errorf("ACK = %+v, error = %v; want successful ACK of %+v", sub.acked, sub.ackErr, sub.delivered[0])
			}
			after, err := memStore.GetTask(context.Background(), task.Metadata.Atespace, task.Metadata.Name)
			if err != nil {
				t.Fatalf("GetTask: %v", err)
			}
			wantStored := before
			if tc.persisted {
				wantStored = computed
			}
			if !proto.Equal(after.Status, wantStored) {
				t.Errorf("stored status = %v, want %v", after.Status, wantStored)
			}
		})
	}
}

type statusFailureControlServer struct {
	*mockControlServer
	createErr   error
	createCalls int
}

func (s *statusFailureControlServer) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	s.createCalls++
	if s.createErr != nil {
		return nil, s.createErr
	}
	return s.mockControlServer.CreateActor(ctx, req)
}

type statusFailureStore struct {
	store.Store
	cancelOnFailure context.CancelFunc
	failure         error
	failures        int
	sub             *statusFailureSubscription
	statuses        []*v1alpha1.TaskStatus
	writeErr        error
}

func (s *statusFailureStore) Subscribe(ctx context.Context, group, consumer string) (store.Subscription, error) {
	sub, err := s.Store.Subscribe(ctx, group, consumer)
	if err != nil {
		return nil, err
	}
	s.sub.Subscription = sub
	return s.sub, nil
}

func (s *statusFailureStore) UpdateTaskStatus(ctx context.Context, atespace, name string, status *v1alpha1.TaskStatus) error {
	s.statuses = append(s.statuses, proto.Clone(status).(*v1alpha1.TaskStatus))
	if len(s.statuses) <= s.failures {
		// Fail before touching the backing store; later attempts write normally.
		s.writeErr = s.failure
		s.sub.record(ctx, "status write failed")
		if s.cancelOnFailure != nil {
			s.cancelOnFailure()
		}
		return s.writeErr
	}
	s.writeErr = s.Store.UpdateTaskStatus(ctx, atespace, name, status)
	s.sub.record(ctx, "status written")
	return s.writeErr
}

type statusFailureSubscription struct {
	store.Subscription
	steps     chan string
	reads     int
	delivered []store.TaskEvent
	acks      int
	acked     store.TaskEvent
	ackErr    error
}

func (s *statusFailureSubscription) record(ctx context.Context, step string) {
	select {
	case s.steps <- step:
	case <-ctx.Done():
	}
}

func (s *statusFailureSubscription) Next(ctx context.Context) (store.TaskEvent, error) {
	s.reads++
	if s.reads > 1 {
		s.record(ctx, "next read")
	}
	ev, err := s.Subscription.Next(ctx)
	if err == nil {
		s.delivered = append(s.delivered, ev)
		s.record(ctx, "delivered")
	}
	return ev, err
}

func (s *statusFailureSubscription) Ack(ctx context.Context, ev store.TaskEvent) error {
	s.acks++
	s.acked = ev
	s.ackErr = s.Subscription.Ack(ctx, ev)
	s.record(ctx, "ack")
	return s.ackErr
}

func TestWorkerDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{actorTemplates: map[string]bool{"doomed-tmpl-0a1b2c3d": true}}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "doomed", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/test/img"},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Actor: "doomed"},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}
	// Drain the reconcile event SaveTask published so only the delete is processed.
	drain, _ := memStore.Subscribe(ctx, "drain", "drain")
	drainCtx, drainCancel := context.WithTimeout(ctx, time.Second)
	_, _ = drain.Next(drainCtx)
	drainCancel()

	if err := memStore.MarkTaskDeleting(ctx, "default", "doomed"); err != nil {
		t.Fatalf("MarkTaskDeleting failed: %v", err)
	}
	marked, err := memStore.GetTask(ctx, "default", "doomed")
	if err != nil {
		t.Fatalf("GetTask after mark failed: %v", err)
	}
	if marked.Status.Phase != v1alpha1.PhaseTerminating {
		t.Fatalf("expected phase Terminating, got %q", marked.Status.Phase)
	}

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := memStore.GetTask(ctx, "default", "doomed"); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := memStore.GetTask(ctx, "default", "doomed"); err == nil {
		t.Fatalf("expected task record to be removed after cleanup")
	}
	if len(mockSrv.deletedActors) != 1 || mockSrv.deletedActors[0] != "doomed" {
		t.Errorf("expected actor 'doomed' deleted, got %v", mockSrv.deletedActors)
	}
	if len(mockSrv.deletedTemplates) != 1 || mockSrv.deletedTemplates[0] != "doomed-tmpl-0a1b2c3d" {
		t.Errorf("expected template deleted, got %v", mockSrv.deletedTemplates)
	}
}

func TestWorkerSkipsReconcileOfTerminatingTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	// Queue a reconcile and then a delete before the worker starts, as when a
	// task is deleted while the controller is still busy with other events.
	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "doomed", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/test/img"},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}
	if err := memStore.MarkTaskDeleting(ctx, "default", "doomed"); err != nil {
		t.Fatalf("MarkTaskDeleting failed: %v", err)
	}

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := memStore.GetTask(ctx, "default", "doomed"); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := memStore.GetTask(ctx, "default", "doomed"); err == nil {
		t.Fatalf("expected task record to be removed after cleanup")
	}
	if len(mockSrv.createdActors) != 0 || len(mockSrv.resumedActors) != 0 {
		t.Errorf("terminating task was reconciled: created %v, resumed %v", mockSrv.createdActors, mockSrv.resumedActors)
	}
}

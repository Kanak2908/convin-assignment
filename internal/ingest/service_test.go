package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func waitForRecordingProcessed(t *testing.T, st *store.Store, callID string, within time.Duration) bool {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(within)

	for time.Now().Before(deadline) {
		var processed bool
		err := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID,
		).Scan(&processed)
		if err != nil {
			t.Fatalf("query recording_processed for %s: %v", callID, err)
		}
		if processed {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestRecordingMarkedProcessedAfterIngest(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// recordingWork is 50ms; allow headroom for scheduling and DB round-trips.
	if !waitForRecordingProcessed(t, st, callID, 500*time.Millisecond) {
		t.Fatalf("recording for call %s was never marked processed", callID)
	}
}

func newTestService(t *testing.T, st *store.Store) *ingest.Service {
	t.Helper()
	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return ingest.New(st, stats.NewCache(), rdb, log)
}

func recordingProcessed(t *testing.T, st *store.Store, callID string) bool {
	t.Helper()
	var processed bool
	err := st.Pool().QueryRow(context.Background(),
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID,
	).Scan(&processed)
	if err != nil {
		t.Fatalf("query recording_processed: %v", err)
	}
	return processed
}

// drainForDeploy is what main should call before exit. Today the service exposes
// no such hook, which is the deploy-time in-flight bug.
func drainForDeploy(t *testing.T, svc *ingest.Service, ctx context.Context) {
	t.Helper()
	type shutdowner interface {
		Shutdown(context.Context) error
	}
	s, ok := any(svc).(shutdowner)
	if !ok {
		t.Fatal("deploy has no way to drain in-flight recordings before exit")
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestInFlightRecordingStillRunningWithoutDrain(t *testing.T) {
	st := testutil.NewStore(t)
	svc := newTestService(t, st)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	var evt ingest.Event
	if err := json.Unmarshal([]byte(body), &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Ingest returns before the 50ms recording work finishes.
	if recordingProcessed(t, st, callID) {
		t.Fatal("expected recording to still be in-flight right after ingest")
	}
}

func TestDeployShutdownDrainsInFlightRecordings(t *testing.T) {
	st := testutil.NewStore(t)
	svc := newTestService(t, st)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	var evt ingest.Event
	if err := json.Unmarshal([]byte(body), &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	drainForDeploy(t, svc, shutdownCtx)

	if !recordingProcessed(t, st, callID) {
		t.Fatal("deploy shutdown exited before in-flight recording finished")
	}
}

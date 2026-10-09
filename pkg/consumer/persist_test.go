package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/cockroachdb/pebble"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
)

// newTestConsumer builds a Consumer backed by two temporary Pebble databases,
// the same pair NewConsumer opens, without needing a firehose connection.
func newTestConsumer(t *testing.T) *Consumer {
	t.Helper()
	dir := t.TempDir()
	uDB, err := pebble.Open(dir+"/jetstream.uncompressed.db", &pebble.Options{})
	if err != nil {
		t.Fatalf("open uncompressed: %v", err)
	}
	cDB, err := pebble.Open(dir+"/jetstream.compressed.db", &pebble.Options{})
	if err != nil {
		t.Fatalf("open compressed: %v", err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	t.Cleanup(func() {
		uDB.Close()
		cDB.Close()
		enc.Close()
	})
	return &Consumer{UncompressedDB: uDB, CompressedDB: cDB, encoder: enc}
}

// persist writes one commit event to both databases exactly as the live path
// does: JSON into the uncompressed DB, a zstd frame into the compressed one,
// under an identical key.
func persist(t *testing.T, c *Consumer, timeUS int64, did, collection string) []byte {
	t.Helper()
	evt := &models.Event{
		Did:    did,
		TimeUS: timeUS,
		Kind:   models.EventKindCommit,
		Commit: &models.Commit{
			Operation:  models.CommitOperationCreate,
			Collection: collection,
			RKey:       fmt.Sprintf("rkey%d", timeUS),
			Record:     json.RawMessage(`{"text":"hello"}`),
		},
	}
	asJSON, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	compBytes := c.encoder.EncodeAll(asJSON, nil)
	if err := c.PersistEvent(context.Background(), evt, asJSON, compBytes); err != nil {
		t.Fatalf("persist: %v", err)
	}
	return compBytes
}

// collect runs ReplayEvents and gathers what it emitted.
func collect(t *testing.T, c *Consumer, compressed bool, cursor int64) (lastSeq int64, payloads [][]byte) {
	t.Helper()
	lastSeq, err := c.ReplayEvents(context.Background(), compressed, cursor, 100000,
		func(ctx context.Context, timeUS int64, did, collection, operation, kind string, get func() []byte) error {
			b := append([]byte(nil), get()...)
			payloads = append(payloads, b)
			return nil
		})
	if err != nil {
		t.Fatalf("ReplayEvents(compressed=%v): %v", compressed, err)
	}
	return lastSeq, payloads
}

// A subscriber passing both a cursor and compress=true used to receive nothing:
// playback iterated the compressed database and then tried to json.Unmarshal
// each zstd frame to read kind/operation for filtering, so every event failed to
// parse and was skipped. The socket stayed open having sent zero frames.
func TestReplayEventsCompressedReturnsEvents(t *testing.T) {
	c := newTestConsumer(t)
	base := time.Now().Add(-time.Minute).UnixMicro()
	want := 5
	for i := 0; i < want; i++ {
		persist(t, c, base+int64(i), fmt.Sprintf("did:plc:test%d", i), "app.bsky.feed.post")
	}

	plainSeq, plain := collect(t, c, false, base)
	if len(plain) != want {
		t.Fatalf("uncompressed playback emitted %d events, want %d", len(plain), want)
	}

	compSeq, comp := collect(t, c, true, base)
	if len(comp) != want {
		t.Fatalf("compressed playback emitted %d events, want %d (this is the bug)", len(comp), want)
	}
	if compSeq != plainSeq {
		t.Errorf("lastSeq differs: compressed %d, uncompressed %d", compSeq, plainSeq)
	}
}

// What a compressed subscriber receives must be a zstd frame that decodes back
// to the same event the uncompressed path would have sent - not JSON, and not
// some other record.
func TestReplayEventsCompressedPayloadIsUsableZstd(t *testing.T) {
	c := newTestConsumer(t)
	base := time.Now().Add(-time.Minute).UnixMicro()
	persist(t, c, base, "did:plc:onlyone", "app.bsky.feed.like")

	_, plain := collect(t, c, false, base)
	_, comp := collect(t, c, true, base)
	if len(plain) != 1 || len(comp) != 1 {
		t.Fatalf("expected one event each, got %d plain and %d compressed", len(plain), len(comp))
	}

	// zstd magic number, so the client's decoder will accept it
	if len(comp[0]) < 4 || comp[0][0] != 0x28 || comp[0][1] != 0xb5 || comp[0][2] != 0x2f || comp[0][3] != 0xfd {
		t.Fatalf("compressed payload is not a zstd frame: % x", comp[0][:min(8, len(comp[0]))])
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	got, err := dec.DecodeAll(comp[0], nil)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(got) != string(plain[0]) {
		t.Errorf("decompressed payload differs from the uncompressed one:\n  got  %s\n  want %s", got, plain[0])
	}
}

// Filters must still apply on the compressed path. They are evaluated from the
// JSON copy, so they have to keep working now that the two reads are split.
func TestReplayEventsCompressedStillCarriesFilterFields(t *testing.T) {
	c := newTestConsumer(t)
	base := time.Now().Add(-time.Minute).UnixMicro()
	persist(t, c, base, "did:plc:a", "app.bsky.feed.post")
	persist(t, c, base+1, "did:plc:b", "app.bsky.graph.follow")

	var kinds, ops, collections []string
	_, err := c.ReplayEvents(context.Background(), true, base, 100000,
		func(ctx context.Context, timeUS int64, did, collection, operation, kind string, get func() []byte) error {
			kinds = append(kinds, kind)
			ops = append(ops, operation)
			collections = append(collections, collection)
			return nil
		})
	if err != nil {
		t.Fatalf("ReplayEvents: %v", err)
	}
	if len(kinds) != 2 {
		t.Fatalf("emitted %d events, want 2", len(kinds))
	}
	for i := range kinds {
		if kinds[i] != models.EventKindCommit {
			t.Errorf("event %d kind = %q, want %q", i, kinds[i], models.EventKindCommit)
		}
		if ops[i] != models.CommitOperationCreate {
			t.Errorf("event %d operation = %q, want %q", i, ops[i], models.CommitOperationCreate)
		}
	}
	if collections[0] != "app.bsky.feed.post" || collections[1] != "app.bsky.graph.follow" {
		t.Errorf("collections not carried through: %v", collections)
	}
}

// An event present in the uncompressed DB but missing its compressed copy must
// be skipped, never sent as JSON to a client decoding zstd. The two databases
// are trimmed independently so their windows can differ at the edges.
func TestReplayEventsSkipsEventMissingItsCompressedCopy(t *testing.T) {
	c := newTestConsumer(t)
	base := time.Now().Add(-time.Minute).UnixMicro()
	persist(t, c, base, "did:plc:keep", "app.bsky.feed.post")
	persist(t, c, base+1, "did:plc:gone", "app.bsky.feed.post")

	// Drop one compressed copy, leaving its JSON behind.
	iter, err := c.UncompressedDB.NewIter(nil)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	var victim []byte
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
		if n == 2 { // keep the first, orphan the second
			victim = append([]byte(nil), iter.Key()...)
			break
		}
	}
	iter.Close()
	if len(victim) == 0 {
		t.Fatal("could not pick a key to orphan")
	}
	if err := c.CompressedDB.Delete(victim, pebble.Sync); err != nil {
		t.Fatalf("delete compressed copy: %v", err)
	}

	_, comp := collect(t, c, true, base)
	if len(comp) != 1 {
		t.Fatalf("emitted %d events, want 1 (the orphaned one must be skipped)", len(comp))
	}
	for _, p := range comp {
		if len(p) >= 1 && p[0] == '{' {
			t.Error("JSON was sent to a compressed subscriber")
		}
	}
}

func eventKey(timeUS int64, did, collection string) []byte {
	return []byte(fmt.Sprintf("%d_%s_%s", timeUS, did, collection))
}

// At the old end of the window TrimEvents deletes the JSON copy before the
// compressed one, so an event can briefly exist only in compressed form. Replay
// must skip it quietly and carry on, not fail the whole replay.
func TestReplayEventsCompressedSkipsEventWhoseJSONWasTrimmed(t *testing.T) {
	c := newTestConsumer(t)
	base := time.Now().Add(-time.Minute).UnixMicro()
	persist(t, c, base, "did:plc:old", "app.bsky.feed.post")
	persist(t, c, base+1, "did:plc:mid", "app.bsky.feed.post")
	persist(t, c, base+2, "did:plc:new", "app.bsky.feed.post")

	// Trim has removed the oldest event's JSON but not yet its compressed copy.
	if err := c.UncompressedDB.Delete(eventKey(base, "did:plc:old", "app.bsky.feed.post"), pebble.Sync); err != nil {
		t.Fatalf("delete json copy: %v", err)
	}

	lastSeq, comp := collect(t, c, true, base)
	if len(comp) != 2 {
		t.Fatalf("emitted %d events, want 2 (the half-trimmed one skipped, the rest delivered)", len(comp))
	}
	if lastSeq != base+2 {
		t.Errorf("lastSeq = %d, want %d", lastSeq, base+2)
	}
}

// Persistence is sequential, so an event still being written is always the
// newest key: its JSON exists, its compressed copy does not yet. Replay must not
// move the cursor past it, so the next pass picks it up once it completes.
func TestReplayEventsCompressedDoesNotPassAnInFlightEvent(t *testing.T) {
	c := newTestConsumer(t)
	base := time.Now().Add(-time.Minute).UnixMicro()
	persist(t, c, base, "did:plc:a", "app.bsky.feed.post")
	persist(t, c, base+1, "did:plc:b", "app.bsky.feed.post")
	persist(t, c, base+2, "did:plc:c", "app.bsky.feed.post")

	// Rewind the newest event to "JSON written, compressed copy still pending".
	inflight := eventKey(base+2, "did:plc:c", "app.bsky.feed.post")
	if err := c.CompressedDB.Delete(inflight, pebble.Sync); err != nil {
		t.Fatalf("delete compressed copy: %v", err)
	}

	lastSeq, comp := collect(t, c, true, base)
	if len(comp) != 2 {
		t.Fatalf("first pass emitted %d events, want 2", len(comp))
	}
	if lastSeq != base+1 {
		t.Fatalf("lastSeq = %d, want %d - the cursor moved past an event still being written", lastSeq, base+1)
	}

	// The write completes; the next pass resumes from just after lastSeq.
	persist(t, c, base+2, "did:plc:c", "app.bsky.feed.post")
	lastSeq2, comp2 := collect(t, c, true, lastSeq+1)
	if len(comp2) != 1 {
		t.Fatalf("second pass emitted %d events, want the 1 that was in flight", len(comp2))
	}
	if lastSeq2 != base+2 {
		t.Errorf("second pass lastSeq = %d, want %d", lastSeq2, base+2)
	}
}

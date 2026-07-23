package etcd

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	goetcdv3 "go.etcd.io/etcd/client/v3"
)

type fakeClientV3 struct {
	watchResponses chan goetcdv3.WatchResponse
	closeCount     atomic.Int32
}

func (c *fakeClientV3) Watch(context.Context, string, ...goetcdv3.OpOption) goetcdv3.WatchChan {
	return c.watchResponses
}

func (c *fakeClientV3) Close() error {
	c.closeCount.Add(1)
	return nil
}

func TestClientV3WatchErrorDoesNotCloseSharedClient(t *testing.T) {
	client := &fakeClientV3{watchResponses: make(chan goetcdv3.WatchResponse, 1)}
	store := &ClientV3{
		ctx:    context.Background(),
		client: client,
	}

	responses := store.Watch("key", make(chan bool))
	client.watchResponses <- goetcdv3.WatchResponse{
		Canceled:        true,
		CompactRevision: 1,
	}

	select {
	case response, ok := <-responses:
		if !ok {
			t.Fatal("watch response channel closed before returning the error")
		}
		if response.Error == nil {
			t.Fatal("watch returned a nil error for a compacted response")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch error")
	}

	select {
	case _, ok := <-responses:
		if ok {
			t.Fatal("watch response channel remained open after the error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch response channel to close")
	}

	if got := client.closeCount.Load(); got != 0 {
		t.Fatalf("shared client was closed %d times", got)
	}
}

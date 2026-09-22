package engine

import (
	"context"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const defaultExchangeWS = "wss://ws-feed.exchange.coinbase.com"

// LiveFeedURL returns the WS endpoint (overridable via EXCHANGE_WS_URL).
func LiveFeedURL() string {
	if u := os.Getenv("EXCHANGE_WS_URL"); u != "" {
		return u
	}
	return defaultExchangeWS
}

// LiveFeedEnabled gates the persistent WebSocket client.
// Defaults ON for long-running hosts; OFF on Vercel/Lambda (use REST instead).
// ENABLE_LIVE_FEED=1 forces WS even on serverless; DISABLE_LIVE_FEED=1 disables all live paths.
func LiveFeedEnabled() bool {
	if os.Getenv("DISABLE_LIVE_FEED") == "1" {
		return false
	}
	if os.Getenv("ENABLE_LIVE_FEED") == "1" {
		return true
	}
	if os.Getenv("VERCEL") == "1" || os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		return false
	}
	return true
}

// RunLiveL2Feed dials the venue WebSocket, applies snapshots/deltas via L2Feed,
// and reconnects with backoff on gaps/errors. Sets wsConnected=1 while live so
// the REST poller / mock producer yield. Cancels when ctx is done.
func RunLiveL2Feed(ctx context.Context, feed *L2Feed) {
	if !LiveFeedEnabled() {
		// Serverless / WS-disabled: REST poller owns freshness.
		if RestFeedEnabled() {
			feed.SetStatus(FeedStatusConnecting)
		} else {
			feed.SetStatus(FeedStatusFallbackMock)
		}
		return
	}

	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		feed.SetStatus(FeedStatusConnecting)
		err := dialAndConsume(ctx, feed)
		atomic.StoreInt32(&wsConnected, 0)
		if ctx.Err() != nil {
			return
		}
		feed.SetStatus(FeedStatusResyncing)
		AddTrace("W", "WRITE", 0, 0, "live feed disconnected: "+errString(err))
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func errString(err error) string {
	if err == nil {
		return "eof"
	}
	return err.Error()
}

func dialAndConsume(ctx context.Context, feed *L2Feed) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	conn, _, err := dialer.DialContext(ctx, LiveFeedURL(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, SubscribeMessage(feed.ProductID)); err != nil {
		return err
	}

	atomic.StoreInt32(&wsConnected, 1)
	feed.SetStatus(FeedStatusLive)
	atomic.StoreInt64(&feed.stats.ConnectedNS, time.Now().UnixNano())
	feed.needSnap = true // require snapshot after (re)connect
	feed.haveSeq = false

	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	})

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		resync, err := feed.HandleMessage(data)
		if err != nil {
			continue
		}
		if resync {
			return fmtError("sequence gap — forcing reconnect")
		}
	}
}

type simpleError string

func (e simpleError) Error() string { return string(e) }
func fmtError(s string) error       { return simpleError(s) }

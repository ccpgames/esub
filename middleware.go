package main

import (
	"context"
	"net/http"
	"time"

	"github.com/rs/xhandler"
	"github.com/rs/xmux"
)

// CloseConnectionHandler -- closes all connections
func CloseConnectionHandler(next xhandler.HandlerC) xhandler.HandlerC {
	return xhandler.HandlerFuncC(
		func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
			r.Close = true
			w.Header().Set("Connection", "close")
			next.ServeHTTPC(ctx, w, r)
		})
}

// MetricHandler -- adds ctx values to keys
func MetricHandler(next xhandler.HandlerC) xhandler.HandlerC {
	return xhandler.HandlerFuncC(
		func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
			// start timing
			start := time.Now().UTC()
			ctx = context.WithValue(ctx, keyStart, start)

			// requested sub key
			key := xmux.Params(ctx).Get("sub")
			ctx = context.WithValue(ctx, keyKey, key)

			// check for provided auth
			token := r.URL.Query().Get("token")
			ctx = context.WithValue(ctx, keyToken, token)

			// run the rest of the chain
			next.ServeHTTPC(ctx, w, r)
		})
}

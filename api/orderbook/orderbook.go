package handler

import (
	"net/http"

	"latency-optimizer/engine"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	engine.HandleOrderBookAPI(w, r)
}

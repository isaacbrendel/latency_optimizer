package handler

import (
	"net/http"

	"latency-optimizer/engine"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	engine.HandleSentimentAPI(w, r)
}

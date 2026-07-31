package main

import (
	"fmt"
	"net/http"
	"os"

	"latency-optimizer/engine"
)

func main() {
	engine.EnsureInitialized()

	if len(os.Args) > 1 && (os.Args[1] == "--bench" || os.Args[1] == "-bench") {
		tradeCounts := []int{1000, 5000, 10000, 50000, 100000}
		subscriberCounts := []int{10, 50, 100, 500, 1000, 2000}
		results, err := engine.RunExperimentSuite(tradeCounts, subscriberCounts, os.Stdout)
		if err != nil {
			fmt.Printf("Benchmark error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Benchmark completed successfully: %d trade points, %d sub points.\n",
			len(results.TradesScaling.Points), len(results.SubscribersScaling.Points))
		return
	}

	http.HandleFunc("/api/orderbook", engine.HandleOrderBookAPI)
	http.HandleFunc("/api/ring-buffer", engine.HandleRingBufferAPI)
	http.HandleFunc("/api/sentiment", engine.HandleSentimentAPI)
	http.HandleFunc("/api/run-experiment", engine.HandleRunExperimentAPI)

	fs := http.FileServer(http.Dir("docs"))
	http.Handle("/", fs)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("====================================================")
	fmt.Println(" Latency Optimizer Live Server Running")
	fmt.Printf(" Port: %s\n", port)
	fmt.Printf(" URL:  http://localhost:%s\n", port)
	fmt.Println("====================================================")

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Printf("Server failed: %v\n", err)
	}
}

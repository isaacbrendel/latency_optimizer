.PHONY: all build run bench test clean fuzz

all: build

build:
	@echo "Building low-latency live server binary..."
	go build -o test_bin .

run: build
	@echo "Starting live server at http://localhost:8080..."
	./test_bin

bench:
	@echo "Executing benchmark suite..."
	go test -bench=. -benchmem -count=5 ./engine/

test:
	@echo "Running race-detector suite..."
	go test -race -count=1 ./engine/

fuzz:
	go test -run=^$$ -fuzz=FuzzRingBufferPublishRead -fuzztime=10s ./engine
	go test -run=^$$ -fuzz=FuzzFixedPointUSD -fuzztime=10s ./engine

clean:
	@echo "Cleaning binaries and temp files..."
	rm -f test_bin bot_state.json server_test_bin

.PHONY: all build run bench test clean

all: build

build:
	@echo "Building low-latency live server binary..."
	go build -o test_bin .

run: build
	@echo "Starting live server at http://localhost:8080..."
	./test_bin

bench:
	@echo "Executing benchmark suite (channels vs lock-free disruptor)..."
	go test -v -bench=. -benchmem

test:
	@echo "Running unit tests..."
	go test -v ./...

clean:
	@echo "Cleaning binaries and temp files..."
	rm -f test_bin bot_state.json

.PHONY: build test integration benchmark stability vet
build:
	go build -trimpath -o bin/go-sync ./cmd/go-sync
	go build -trimpath -o bin/go-sync-server ./cmd/go-sync-server
test:
	go test -race ./...
integration:
	go test -race -count=1 -timeout=30m -tags=integration ./...
benchmark:
	go test -run '^$$' -bench . -benchmem -count=5 ./internal/event ./internal/queue ./internal/server
stability:
	GO_SYNC_STABILITY_DURATION=$${GO_SYNC_STABILITY_DURATION:-2m} go test -race -count=1 -timeout=10m -tags=stability ./internal/queue ./internal/server
vet:
	go vet ./...

.PHONY: run build css test vet smoke
run:
	go run ./cmd/maxstore
css:
	npm run build:css
build: css
	go build -trimpath -o bin/maxstore ./cmd/maxstore
test:
	npm test
	go test -race ./...
vet:
	go vet ./...
smoke:
	python3 scripts/s3-smoke.py

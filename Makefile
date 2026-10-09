# Everything runs in the pinned Go image, so local results match CI and the release.
GO_VERSION := $(shell cat .go-version)
GO_IMAGE   := golang:$(GO_VERSION)-bookworm
DOCKER     := docker run --rm -v "$(CURDIR)":/src -w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE)

.PHONY: test release
test:
	$(DOCKER) sh -c 'test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }; go vet ./... && go test -race -count=1 ./...'

# make release VERSION=v1.0.0 -> dist/emak-jobs-v1.0.0-linux-amd64.tar.gz (+ .sha256)
release:
	$(DOCKER) scripts/release.sh $(VERSION) dist

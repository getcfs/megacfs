SHA := $(shell git rev-parse --short HEAD)
VERSION := $(shell cat VERSION)
BUILDPATH := "build/"
LD_FLAGS := -s -w
GOVERSION := $(shell go version | sed -e 's/ /-/g')
BDATE := $(shell date -u +%Y-%m-%d.%H:%M:%S)

test:
	go test $(shell go list ./... | grep -v /vendor/)

build:
	mkdir -p $(BUILDPATH)
	go build -i -v --ldflags "$(LD_FLAGS)" -o build/ring github.com/getcfs/megacfs/ring
	go build -i -v -o build/cfs --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.cfsVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/cfs
	go build -i -v -o build/cfsd --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.cfsVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/cfsd
	go build -i -v -o build/cfsadm --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.cfsVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/cfsadm

clean:
	rm -rf $(BUILDPATH)

install:
	go install -v $(shell go list ./... | grep -v /vendor/)

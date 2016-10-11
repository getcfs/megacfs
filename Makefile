SHA := $(shell git rev-parse --short HEAD)
VERSION := $(shell cat VERSION)
SRCPATH := "oort/"
BUILDPATH := "build/"
LD_FLAGS := -s -w
GOVERSION := $(shell go version | sed -e 's/ /-/g')
BDATE := $(shell date -u +%Y-%m-%d.%H:%M:%S)

test:
	go test $(shell glide novendor)

build:
	mkdir -p $(BUILDPATH)
	go build -i -v --ldflags "$(LD_FLAGS)" -o build/oort-cli github.com/getcfs/megacfs/oort/oort-cli
	go build -i -v --ldflags "$(LD_FLAGS)" -o build/oort-bench github.com/getcfs/megacfs/oort/oort-bench
	go build -i -v --ldflags "$(LD_FLAGS)" -o build/ring github.com/getcfs/megacfs/ring
	go build -i -v -o build/oort-valued --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.oortVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/oort/oort-valued
	go build -i -v -o build/oort-groupd --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.oortVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/oort/oort-groupd
	go build -i -v -o build/synd --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.syndVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/syndicate/synd
	go build -i -v -o build/syndicate-client --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.syndVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/syndicate/syndicate-client
	go build -i -v --ldflags "$(LD_FLAGS)" -o build/cfsdvp github.com/getcfs/megacfs/formic/cfsdvp
	go build -i -v -o build/cfs --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.cfsVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/formic/cfs
	go build -i -v -o build/formicd --ldflags " $(LD_FLAGS) \
			-X main.commitVersion=$(SHA) \
			-X main.formicdVersion=$(VERSION) \
			-X main.goVersion=$(GOVERSION) \
			-X main.buildDate=$(BDATE)" github.com/getcfs/megacfs/formic/formicd

darwin: export GOOS=darwin
darwin:
	go build -i -v --ldflags "$(LD_FLAGS)" -o build/cfs.osx github.com/getcfs/megacfs/formic/cfs

compact:
	upx -q -1 build/synd
	upx -q -1 build/oort-valued
	upx -q -1 build/oort-groupd
	upx -q -1 build/formicd

clean:
	rm -rf $(BUILDPATH)

install:
	go install -v $(shell glide novendor)

up:
	glide up -u --strip-vcs

SHA := $(shell git rev-parse --short HEAD)
VERSION := $(shell cat VERSION)
ITTERATION := $(shell date +%s)
LOCALPKGS :=  $(shell go list ./... | grep -v /vendor/)

deps:
	go get $(LOCALPKGS)

build:
	mkdir -p packaging/output
	mkdir -p packaging/root/usr/local/bin
	go build -i -v -o packaging/root/usr/local/bin/oort-valued --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.oortVersion=$(shell git rev-parse HEAD) \
		-X main.valuestoreVersion=$(shell git -C $$GOPATH/src/github.com/gholt/store rev-parse HEAD) \
		-X main.cmdctrlVersion=$(shell git -C $$GOPATH/src/github.com/pandemicsyn/cmdctrl rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)" github.com/getcfs/megacfs/oort/oort-valued
	go build -i -v -o packaging/root/usr/local/bin/oort-groupd --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.oortVersion=$(shell git rev-parse HEAD) \
		-X main.valuestoreVersion=$(shell git -C $$GOPATH/src/github.com/gholt/store rev-parse HEAD) \
		-X main.cmdctrlVersion=$(shell git -C $$GOPATH/src/github.com/pandemicsyn/cmdctrl rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)" github.com/getcfs/megacfs/oort/oort-groupd
	go build -i -v -o packaging/root/usr/local/bin/oort-cli github.com/getcfs/megacfs/oort/oort-cli

clean:
	rm -rf packaging/output
	rm -f packaging/root/usr/local/bin/oort-valued
	rm -f packaging/root/usr/local/bin/oort-groupd
	rm -f packaging/root/usr/local/bin/oort-cli

install: build
	mkdir -p /var/lib/oort-value/ring /var/lib/oort-value/data
	mkdir -p /var/lib/oort-group/ring /var/lib/oort-group/data
	install -t $(GOPATH)/bin packaging/root/usr/local/bin/*

test:
	go test ./...

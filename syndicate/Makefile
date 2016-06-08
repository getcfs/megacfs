SHA := $(shell git rev-parse --short HEAD)
VERSION := $(shell cat VERSION)
ITTERATION := $(shell date +%s)
DDIR = /etc/syndicate
RINGDIR = /etc/syndicate/ring
VV = $(shell ringver /etc/syndicate/ring/valuestore.ring)
GV = $(shell ringver /etc/syndicate/ring/groupstore.ring)

deps:
	go get -u ./...

test:
	go get ./...
	go test -i ./...
	go test ./...

build:
	mkdir -p packaging/output
	mkdir -p packaging/root/usr/local/bin
	go build -i -v -o packaging/root/usr/local/bin/synd --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.syndVersion=$(shell git rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)" github.com/getcfs/megacfs/syndicate/synd 
	go build -i -v -o packaging/root/usr/local/bin/syndicate-client --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.syndicateClientVersion=$(shell git rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)"  github.com/getcfs/megacfs/syndicate/syndicate-client

clean:
	rm -rf packaging/output
	rm -f packaging/root/usr/local/bin/synd
	rm -f packaging/root/usr/local/bin/syndicate-client

install:
	#install -t /usr/local/bin packaging/root/usr/local/bin/synd
	go install --ldflags " \
		-X main.ringVersion=$(RINGVERSION) \
		-X main.syndVersion=$(shell git rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)" github.com/getcfs/megacfs/syndicate/synd 
	go install --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.syndicateClientVersion=$(shell git rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)"  github.com/getcfs/megacfs/syndicate/syndicate-client

run:
	go run synd/*.go

ring:
	go get github.com/gholt/ring/ring
	go install github.com/gholt/ring/ring
	mkdir -p $(RINGDIR)
	ring $(RINGDIR)/valuestore.builder create replicas=1 config-file=$(DDIR)/valuestore.toml
	ring $(RINGDIR)/valuestore.builder add active=true capacity=1000 tier0=removeme
	ring $(RINGDIR)/valuestore.builder ring
	ring $(RINGDIR)/groupstore.builder create replicas=1 config-file=$(DDIR)/groupstore.toml
	ring $(RINGDIR)/groupstore.builder add active=true capacity=1000 tier0=removeme
	ring $(RINGDIR)/groupstore.builder ring
	$(MAKE) ringversion

ringversion:
	go get github.com/pandemicsyn/ringver
	go install github.com/pandemicsyn/ringver
	cp -av $(RINGDIR)/valuestore.ring $(RINGDIR)/$(VV)-valuestore.ring
	cp -av $(RINGDIR)/valuestore.builder $(RINGDIR)/$(VV)-valuestore.builder
	cp -av $(RINGDIR)/groupstore.ring $(RINGDIR)/$(GV)-groupstore.ring
	cp -av $(RINGDIR)/groupstore.builder $(RINGDIR)/$(GV)-groupstore.builder

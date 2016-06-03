test:
	go test $(shell glide novendor)

install:
	go install -v $(shell glide novendor)

up:
	glide up -u

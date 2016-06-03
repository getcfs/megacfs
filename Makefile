test:
	go test $(shell glide novendor)

install:
	go install -v $(shell glide novendor)

up:
	glide up -u

# Example of updating a single dependency (and its dependencies):
#   glide get -s -u -v github.com/gholt/store

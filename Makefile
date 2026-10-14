GO =	go

all: vet test
check: test

vet:
	${GO} vet ./...

test:
	${GO} test ./...

.PHONY: all check test

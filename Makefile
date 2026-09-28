GO ?= go
BIN_DIR ?= bin

.PHONY: all validator clean

all: validator

validator:
	$(GO) build -o $(BIN_DIR)/validator ./cmd/validator

clean:
	rm -rf $(BIN_DIR)

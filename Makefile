PROTO_SRC_DIR := $(shell pwd)/proto
PROTOS := importer exporter #store

gen:
	for proto in $(PROTOS); do \
		cp $(PROTO_SRC_DIR)/$$proto.proto .; \
		mkdir -p ./pkg/$$proto/; \
		docker run --rm -ti \
			-v `pwd`:/app \
			-w /app \
			rvolosatovs/protoc \
				--proto_path=/app \
				--go_out=./pkg/$$proto/ \
				--go_opt=paths=source_relative,M$$proto.proto=github.com/PlakarKorp/go-kloset-sdk/$$proto \
				--go-grpc_out=./pkg/$$proto/ \
				--go-grpc_opt=paths=source_relative,M$$proto.proto=github.com/PlakarKorp/go-kloset-sdk/$$proto \
				/app/$$proto.proto; \
	done
	rm -f ./importer.proto ./exporter.proto ./store.proto

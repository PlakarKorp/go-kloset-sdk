PROTO_SRC_DIR := $(shell pwd)/proto
IMPORTER_PROTO := $(PROTO_SRC_DIR)/importer.proto
EXPORTER_PROTO := $(PROTO_SRC_DIR)/exporter.proto
STORE_PROTO := $(PROTO_SRC_DIR)/store.proto

gen:
	cp $(IMPORTER_PROTO) .
	cp $(EXPORTER_PROTO) .
	cp $(STORE_PROTO) .

	mkdir -p ./pkg/importer/ ./pkg/exporter/ ./pkg/store/

	docker run \
		--rm -ti \
		-v `pwd`:/app \
		-w /app \
		rvolosatovs/protoc \
			--proto_path=/app \
			--go_out=./pkg/importer/ \
			--go_opt=paths=source_relative,Mimporter.proto=github.com/PlakarKorp/go-kloset-sdk/importer\
			--go-grpc_out=./pkg/importer/ \
			--go-grpc_opt=paths=source_relative,Mimporter.proto=github.com/PlakarKorp/go-kloset-sdk/importer \
			/app/importer.proto

	docker run \
		--rm -ti \
		-v `pwd`:/app \
		-w /app \
		rvolosatovs/protoc \
			--proto_path=/app \
			--go_out=./pkg/exporter/ \
			--go_opt=paths=source_relative,Mexporter.proto=github.com/PlakarKorp/go-kloset-sdk/exporter\
			--go-grpc_out=./pkg/exporter/ \
			--go-grpc_opt=paths=source_relative,Mexporter.proto=github.com/PlakarKorp/go-kloset-sdk/exporter \
			/app/exporter.proto

	docker run \
	 --rm -ti \
	 -v `pwd`:/app \
	 -w /app \
	 rvolosatovs/protoc \
		  --proto_path=/app \
		  --go_out=./pkg/store/ \
		  --go_opt=paths=source_relative,Mstore.proto=github.com/PlakarKorp/go-kloset-sdk/store\
		  --go-grpc_out=./pkg/store/ \
		  --go-grpc_opt=paths=source_relative,Mstore.proto=github.com/PlakarKorp/go-kloset-sdk/store \
		  /app/store.proto

	rm -f ./importer.proto ./exporter.proto ./store.proto
PROTO_SRC_DIR := $(shell pwd)/proto
PROTOS := importer exporter store
HOME := $(shell echo $$HOME)/

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

all: impor expor storage iphoto

impor:
	go build ./tests/fsImporter/main.go
	rm -rf $(HOME).config/plakar/plugins/importer/fs-v1.0.0.ptar
	mv main fs-v1.0.0.ptar
	mv fs-v1.0.0.ptar $(HOME).config/plakar/plugins/importer/

expor:
	go build ./tests/fsExporter/main.go
	rm -rf $(HOME).config/plakar/plugins/exporter/fs-v1.0.0.ptar
	mv main fs-v1.0.0.ptar
	mv fs-v1.0.0.ptar $(HOME).config/plakar/plugins/exporter/

storage:
	go build ./tests/fsStorage/
	rm -rf $(HOME).config/plakar/plugins/storage/fs-v1.0.0.ptar
	mv fsStorage fs-v1.0.0.ptar
	mv fs-v1.0.0.ptar $(HOME).config/plakar/plugins/storage/

iphoto:
	go build ./tests/iphoto/iphoto.go
	rm -rf $(HOME).config/plakar/plugins/importer/iphoto-v1.0.0.ptar
	mv iphoto iphoto-v1.0.0.ptar
	mv iphoto-v1.0.0.ptar $(HOME).config/plakar/plugins/importer/

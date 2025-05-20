gen:
	cp /Users/niluje/dev/plakar/plakar/importer.proto .

	docker run \
		--rm -ti \
		-v `pwd`:/app \
		-w /app \
		rvolosatovs/protoc \
			--proto_path=/app \
			--go_out=./pkg/importer/ \
			--go_opt=paths=source_relative,Mimporter.proto=github.com/PlakarKorp/go-plakar-sdk/importer\
			--go-grpc_out=./pkg/importer/ \
			--go-grpc_opt=paths=source_relative,Mimporter.proto=github.com/PlakarKorp/go-plakar-sdk/importer \
			/app/importer.proto

	rm -f ./importer.proto
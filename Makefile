gen:
	cp /Users/niluje/dev/plakar/plakar/importer.proto .

	docker run \
		--rm -ti \
		-v `pwd`:/app \
		-w /app \
		rvolosatovs/protoc \
			--proto_path=/app \
			--go_out=. \
			--go_opt=paths=source_relative \
			--go-grpc_out=. \
			--go-grpc_opt=paths=source_relative \
			/app/importer.proto
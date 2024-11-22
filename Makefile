gen:
	cp /home/peralban/Plakar/SDK/proto/importer.proto .

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

	rm -f ./importer.proto
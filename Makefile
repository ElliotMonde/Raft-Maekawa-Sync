PROTO_DIR = api

proto:
	protoc --go_out =. --go_opt=paths=source_relative \
			--go-grpc_out =. --go-grpc_out=paths=source_relative \
			$(PROTO_DIR)/*.proto

clean:
	rm -rf api/**/*.pb.go
package pub2subpb

//go:generate protoc -I ../../protobuf ../../protobuf/pub2sub/pub2sub.proto --go_out=plugins=grpc:../../../../../ --twirp_out=../../../../../

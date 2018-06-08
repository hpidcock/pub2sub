package pub2subpb

//off go:generate protoc -I ../../protobuf ../../protobuf/pub2sub/pub2sub.proto --go_out=plugins=grpc:../../../../../

//go:generate protoc -I . -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc:$GOPATH/src pub2sub/pub2sub.proto

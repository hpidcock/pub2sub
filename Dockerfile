FROM golang:1.10 AS build
WORKDIR /go/src/github.com/hpidcock/pub2sub
ADD cmd cmd
ADD pkg pkg
ADD vendor vendor
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/publisher
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/distributor
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/planner
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/executor
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/subscriber

FROM alpine
RUN apk add --no-cache ca-certificates
WORKDIR /usr/local/bin
COPY --from=build /go/src/github.com/hpidcock/pub2sub/publisher publisher
COPY --from=build /go/src/github.com/hpidcock/pub2sub/distributor distributor
COPY --from=build /go/src/github.com/hpidcock/pub2sub/planner planner
COPY --from=build /go/src/github.com/hpidcock/pub2sub/executor executor
COPY --from=build /go/src/github.com/hpidcock/pub2sub/subscriber subscriber

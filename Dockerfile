FROM golang:1.11 AS build
WORKDIR /root/
ADD . . 
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/publisher
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/distributor
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/planner
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/executor
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo github.com/hpidcock/pub2sub/cmd/subscriber

FROM alpine
RUN apk add --no-cache ca-certificates
WORKDIR /usr/local/bin
COPY --from=build /root/publisher publisher
COPY --from=build /root/distributor distributor
COPY --from=build /root/planner planner
COPY --from=build /root/executor executor
COPY --from=build /root/subscriber subscriber

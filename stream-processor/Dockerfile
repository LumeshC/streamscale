FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY stream-processor ./stream-processor
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/stream-processor ./stream-processor

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/stream-processor /usr/local/bin/stream-processor
USER nobody
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/stream-processor"]

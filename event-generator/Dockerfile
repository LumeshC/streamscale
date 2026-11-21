FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY event-generator ./event-generator
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/event-generator ./event-generator

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/event-generator /usr/local/bin/event-generator
USER nobody
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/event-generator"]

FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY analytics-api ./analytics-api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/analytics-api ./analytics-api

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/analytics-api /usr/local/bin/analytics-api
USER nobody
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/analytics-api"]

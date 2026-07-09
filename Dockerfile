# --- build stage ---
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY third_party/ ./third_party/
RUN CGO_ENABLED=0 GOFLAGS=-mod=mod GOPROXY=off GOOS=linux go build -ldflags="-s -w" -o /orbitcp .

# --- runtime stage: minimal Alpine image ---
FROM alpine:3.20
RUN adduser -D -u 10001 orbitcp
COPY --from=build /orbitcp /usr/local/bin/orbitcp
USER orbitcp
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orbitcp"]

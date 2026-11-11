# --- build stage ---
FROM golang:1.22-alpine AS build
WORKDIR /src
# The only dependency (lib/pq) is vendored under third_party/ via a
# `replace` directive in go.mod, so there is nothing to download.
COPY go.mod ./
COPY third_party ./third_party
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=mod GOPROXY=off go build -ldflags="-s -w" -o /orbitcp .

# --- runtime stage: minimal Alpine image ---
FROM alpine:3.20
RUN adduser -D -u 10001 orbitcp
COPY --from=build /orbitcp /usr/local/bin/orbitcp
USER orbitcp
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orbitcp"]

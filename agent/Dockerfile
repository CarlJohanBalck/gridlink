# Build context is the repo root (see docker-compose.yml): the workspace
# spans four modules, so the whole tree is needed to resolve gridlink/* deps.
#
# DEV ONLY. A containerized agent drives jobs through the host's Docker
# socket, which is root-equivalent access; real provider installs run the
# agent binary directly on the host instead.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Warm the module cache before copying source so code edits don't re-download.
COPY go.work go.work.sum ./
COPY contracts/go.mod contracts/go.sum contracts/
COPY agent/go.mod agent/go.sum agent/
COPY coordinator/go.mod coordinator/go.sum coordinator/
COPY gateway/go.mod gateway/go.sum gateway/
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/agent ./agent/cmd/agent

FROM alpine:3.20
# Runs as root: the mounted /var/run/docker.sock is root-owned, and this
# image exists only for local docker-compose development.
COPY --from=build /out/agent /usr/local/bin/agent
ENTRYPOINT ["/usr/local/bin/agent"]

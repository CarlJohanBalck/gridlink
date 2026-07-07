# Build context is the repo root (see docker-compose.yml): the workspace
# spans four modules, so the whole tree is needed to resolve gridlink/* deps.
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
RUN CGO_ENABLED=0 go build -o /out/coordinator ./coordinator/cmd/coordinator

FROM alpine:3.20
RUN adduser -D -u 10001 gridlink
USER gridlink
COPY --from=build /out/coordinator /usr/local/bin/coordinator
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/coordinator"]

# Builds all three binaries into one small image; docker-compose picks the
# entrypoint per service.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/metadata ./cmd/metadata \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/storage ./cmd/storage \
 && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
# Owned by nonroot so empty named volumes mounted at /data inherit write access.
COPY --from=build --chown=65532:65532 /data /data
USER nonroot:nonroot

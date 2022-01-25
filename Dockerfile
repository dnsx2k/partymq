FROM golang:1.16 AS build_base
WORKDIR /src
COPY go.mod .
COPY go.sum .
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN go mod download
RUN go get -u github.com/psampaz/go-mod-outdated
RUN go list -u -m -json all | go-mod-outdated -direct -update

FROM build_base as builder
WORKDIR /src
COPY . .
WORKDIR /src/cmd
RUN go build -ldflags="-w -s" -installsuffix cgo -tags=jsoniter -o /out/worker .

FROM debian:stretch-slim as runner
ENV DEBIAN_FRONTEND noninteractive
RUN adduser --disabled-password --no-create-home --gecos '' appuser
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates net-tools curl \
    && apt-get clean -y \
    && apt-get autoremove -y \
    && rm -rf /tmp/* /var/tmp/* \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir /app && chown appuser:appuser /app
WORKDIR /app
USER appuser

FROM runner
WORKDIR /app
COPY --from=builder --chown=appuser /out/worker .
EXPOSE 8085
ENV GIN_MODE=release
ENV LOG_SEVERITY=debug

ENTRYPOINT ["/app/worker"]

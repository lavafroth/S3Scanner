FROM golang:1.18-alpine AS builder

ARG VERSION=dev

WORKDIR /app
COPY .. /app

RUN go build -ldflags="-X 'main.version=${VERSION}'" -o /s3scanner .
RUN go build -o /mqingest ./cmd/mqingest/

FROM alpine

LABEL org.opencontainers.image.title=S3Scanner
LABEL org.opencontainers.image.description="Scan for misconfigured S3 buckets across S3-compatible APIs! "
LABEL org.opencontainers.image.vendor="Dan Salmon"
LABEL org.opencontainers.image.licenses=MIT
LABEL org.opencontainers.image.url=https://github.com/sa7mon/S3Scanner
LABEL org.opencontainers.image.source=https://github.com/sa7mon/S3Scanner
LABEL org.opencontainers.image.documentation=https://github.com/sa7mon/S3Scanner

COPY --from=builder /s3scanner /
COPY --from=builder /mqingest /
ENV PATH="/"
ENTRYPOINT ["/s3scanner"]


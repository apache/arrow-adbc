FROM alpine:3

ARG GO

RUN apk add --no-cache \
    build-base \
    cmake \
    ninja \
    pkgconfig \
    bash \
    git \
    ca-certificates \
    python3 \
    py3-pip \
    openssl-dev \
    zlib-dev \
    musl-dev \
    libstdc++ \
    go \
    linux-headers \
    postgresql-dev \
    pkgconf \
    sqlite-dev \
    file \
    findutils

# syntax=docker/dockerfile:1

# Use BuildKit's build-time cache mounts, it makes a huge difference on rebuilds.
# - https://vsupalov.com/buildkit-cache-mount-dockerfile/
# - https://github.com/FernandoMiguel/Buildkit#mounttypecache

# Stage 1: Build wheel package
FROM python:3.14-slim-bookworm AS build

# Configure operating system.
ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=linux

# Enable concurrent multi-arch builds.
# https://github.com/docker/buildx/issues/549#issuecomment-1788297892
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

# Install Git, it is needed for `versioningit`.
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,id=apt-cache-${TARGETARCH}${TARGETVARIANT},target=/var/cache/apt \
    --mount=type=cache,id=apt-lists-${TARGETARCH}${TARGETVARIANT},target=/var/lib/apt \
    true \
    && apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests --yes git

# Provide sources.
ARG BUILD=/usr/src
COPY . ${BUILD}

# Build package.
RUN pip install build
RUN python -m build --wheel ${BUILD}


FROM python:3.14-slim-bookworm AS package

LABEL org.opencontainers.image.source="https://github.com/crate/cratedb-toolkit" \
      org.opencontainers.image.title="cratedb-toolkit (full)" \
      org.opencontainers.image.description="CrateDB Toolkit \"full\" image" \
      org.opencontainers.image.licenses="AGPL-3.0-only OR EUPL-1.2"

# Configure operating system.
ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=linux

# Enable concurrent multi-arch builds.
# https://github.com/docker/buildx/issues/549#issuecomment-1788297892
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

# Configure build environment.
ENV PIP_ROOT_USER_ACTION=ignore
ENV UV_COMPILE_BYTECODE=true
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV UV_SYSTEM_PYTHON=true

# Install the `uv` package manager.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Configure runtime environment.
ENV PATH=/root/.local/bin:$PATH

# Provide wheel package.
ARG BUILD=/usr/src
COPY --from=build ${BUILD}/dist/*.whl /tmp

# Install package.
RUN \
    --mount=type=cache,id=pip-${TARGETARCH}${TARGETVARIANT},target=/root/.cache/pip \
    --mount=type=cache,id=uv-${TARGETARCH}${TARGETVARIANT},target=/root/.cache/uv \
    true \
    && WHEEL="$(ls /tmp/*.whl)" \
    && uv pip install --upgrade "cratedb-toolkit[full] @ file://${WHEEL}"

# Install curl, for running Compose health checks.
RUN --mount=type=cache,id=apt-cache-${TARGETARCH}${TARGETVARIANT},target=/var/cache/apt \
    --mount=type=cache,id=apt-lists-${TARGETARCH}${TARGETVARIANT},target=/var/lib/apt \
    true \
    && apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests --yes curl

# Install Supercronic, for scheduling periodic tasks.
# Latest releases available at https://github.com/aptible/supercronic/releases.
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.45/supercronic-linux-${TARGETARCH} \
    SUPERCRONIC=supercronic-linux-${TARGETARCH}
RUN case "${TARGETARCH}" in \
      amd64)  SUPERCRONIC_SHA1SUM=e894b193bea75a5ee644e700c59e30eedc804cf7 ;; \
      386)    SUPERCRONIC_SHA1SUM=9389486a0727197fd37573adfe0575f520253de2 ;; \
      arm)    SUPERCRONIC_SHA1SUM=dce6c3ab96923780f6ca9ad26a5406a1a8fcb04b ;; \
      arm64)  SUPERCRONIC_SHA1SUM=20ce6dace414a64f0632f4092d6d3745db6085ad ;; \
      *)      echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac \
    && curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
    && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

# Copy `selftest.sh` to the image.
COPY release/oci-full/selftest.sh /usr/local/bin
RUN chmod +x /usr/local/bin/selftest.sh

# syntax=docker/dockerfile:1

# Use BuildKit's build-time cache mounts, it makes a huge difference on rebuilds.
# - https://vsupalov.com/buildkit-cache-mount-dockerfile/
# - https://github.com/FernandoMiguel/Buildkit#mounttypecache

# Stage 1: Build wheel package
FROM python:3.12-slim-bookworm AS build

ARG BUILD=/usr/src

# Configure operating system
ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=linux

# Install Git, it is needed for `versioningit`.
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,id=apt-cache,target=/var/cache/apt --mount=type=cache,id=apt-library,target=/var/lib/apt \
    true \
    && apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests --yes git

# Build package.
RUN pip install build
COPY . ${BUILD}
RUN python -m build --wheel ${BUILD}


FROM python:3.12-slim-bookworm AS package

LABEL org.opencontainers.image.source="https://github.com/crate/cratedb-toolkit" \
      org.opencontainers.image.title="cratedb-toolkit (full)" \
      org.opencontainers.image.description="CrateDB Toolkit \"full\" image with all extras" \
      org.opencontainers.image.licenses="AGPL-3.0-only OR EUPL-1.2"

ARG BUILD=/usr/src

# Configure operating system.
ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=linux

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
COPY --from=build ${BUILD}/dist/*.whl /tmp

# Install package.
RUN \
    --mount=type=cache,id=pip,target=/root/.cache/pip \
    --mount=type=cache,id=uv,target=/root/.cache/uv \
    true \
    && WHEEL="$(ls /tmp/*.whl)" \
    && uv tool install --upgrade "cratedb-toolkit[all] @ file://${WHEEL}"

# Copy `selftest.sh` to the image.
COPY release/oci-full/selftest.sh /usr/local/bin
RUN chmod +x /usr/local/bin/selftest.sh

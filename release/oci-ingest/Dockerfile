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
RUN --mount=type=cache,target=/var/cache/apt --mount=type=cache,target=/var/lib/apt \
    true \
    && apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests --yes git

# Build package.
RUN pip install build
RUN python -m build --version
COPY . ${BUILD}
RUN python -m build --wheel ${BUILD}


FROM python:3.12-slim-bookworm AS package

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

# Configure runtime environment.
ENV PATH=/root/.local/bin:$PATH

# Install PyODBC drivers for MSSQL and PostgreSQL.
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,id=apt-cache,target=/var/cache/apt --mount=type=cache,id=apt-library,target=/var/lib/apt \
    true \
    && apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests --yes curl gpg
RUN \
  curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg && \
  curl -sSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN --mount=type=cache,id=apt-cache,target=/var/cache/apt --mount=type=cache,id=apt-library,target=/var/lib/apt \
    true \
    && export ACCEPT_EULA='Y' \
    && apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests --yes build-essential libpq-dev unixodbc-dev msodbcsql18 odbc-postgresql

#    && apt-get install --no-install-recommends --no-install-suggests --yes curl gcc libpq-dev build-essential unixodbc-dev g++ apt-transport-https msodbcsql18 odbc-postgresql

# Update `odbcinst.ini` to make sure full path to driver is listed, and set CommLog to 0.
# i.e disables any communication logs to be written to files
RUN \
    true \
    && sed 's/Driver=psql/Driver=\/usr\/lib\/x86_64-linux-gnu\/odbc\/psql/;s/CommLog=1/CommLog=0/' /etc/odbcinst.ini > /tmp/temp.ini \
    && mv -f /tmp/temp.ini /etc/odbcinst.ini

# Provide wheel package.
COPY --from=build ${BUILD}/dist/*.whl /tmp

# Install package.
RUN \
    --mount=type=cache,id=pip,target=/root/.cache/pip \
    --mount=type=cache,id=uv,target=/root/.cache/uv \
    true \
    && pip install uv \
    && uv tool install --upgrade $(ls -1 /tmp/*.whl)[io-ingestr]

# Copy `selftest.sh` to the image.
COPY release/oci-ingest/selftest.sh /usr/local/bin

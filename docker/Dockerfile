# syntax=docker/dockerfile:1
#
# Aerospike aql - Docker image.
#
# https://github.com/aerospike/aql
#
# Supports linux/amd64 and linux/arm64.
# Driven by docker/docker-build.sh, which passes the version-specific URLs +
# SHA256s as build args (and optionally an in-context .deb via AQL_LOCAL_PKG*).
# Build directly only if you set those args yourself; otherwise use the script.
#
# Base: ubuntu:24.04, the distro the .deb is built on, so its Depends resolve
# against the packages they were compiled against. It is also the base the
# legacy aerospike-tools image used and the one aerospike-server.docker
# publishes — apt installs the .deb and anything it (transitively) needs.
#

ARG AQL_AMD64_URL=""
ARG AQL_AMD64_SHA256=""
ARG AQL_ARM64_URL=""
ARG AQL_ARM64_SHA256=""

FROM ubuntu:24.04@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254

ARG AQL_AMD64_URL
ARG AQL_AMD64_SHA256
ARG AQL_ARM64_URL
ARG AQL_ARM64_SHA256
ARG AQL_LOCAL_PKG=""
ARG AQL_LOCAL_PKG_AMD64=""
ARG AQL_LOCAL_PKG_ARM64=""

LABEL org.opencontainers.image.title="Aerospike aql" \
      org.opencontainers.image.description="aql, an SQL-like query tool for Aerospike database." \
      org.opencontainers.image.source="https://github.com/aerospike/aql" \
      org.opencontainers.image.base.name="docker.io/library/ubuntu:24.04" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="Aerospike"

# glibc otherwise resolves the POSIX locale, whose ANSI_X3.4-1968 charmap strips
# non-ASCII from tool output. ENV rather than an entrypoint export, so docker exec,
# --entrypoint overrides and K8s pod specs inherit it; the pre-13.0.3 wrapper only
# covered its own process. C.utf8 is glibc-builtin on trixie: no locales package,
# no size cost. LC_CTYPE is pinned alongside LANG deliberately -- C.utf8 is the
# image's only UTF-8 locale, so -e LANG=<ungenerated> would fall back to ASCII.
# Precedence is LC_ALL > LC_CTYPE > LANG, so override the ctype via LC_ALL or
# LC_CTYPE. Deviates from python/postgres, which set LANG only and ship locales.
ENV LANG=C.UTF-8 \
    LC_CTYPE=C.UTF-8

SHELL ["/bin/bash", "-Eeuo", "pipefail", "-c"]

# hadolint ignore=DL3008
RUN --mount=type=bind,source=.,target=/build-ctx \
    export DEBIAN_FRONTEND=noninteractive && \
    ARCH="$(dpkg --print-architecture)" && \
    if [ "${ARCH}" = "amd64" ]; then \
        PKG_URL="${AQL_AMD64_URL}"; PKG_SHA="${AQL_AMD64_SHA256}"; \
        LOCAL_PKG="${AQL_LOCAL_PKG_AMD64:-${AQL_LOCAL_PKG}}"; \
    else \
        PKG_URL="${AQL_ARM64_URL}"; PKG_SHA="${AQL_ARM64_SHA256}"; \
        LOCAL_PKG="${AQL_LOCAL_PKG_ARM64:-${AQL_LOCAL_PKG}}"; \
    fi && \
    apt-get update -y && \
    if [ -n "${LOCAL_PKG}" ] && [ -f "/build-ctx/${LOCAL_PKG}" ]; then \
        cp "/build-ctx/${LOCAL_PKG}" /tmp/aql.deb; \
    else \
        apt-get install -y --no-install-recommends curl ca-certificates && \
        curl -fsSL --retry 3 --retry-delay 3 "${PKG_URL}" -o /tmp/aql.deb && \
        apt-get purge -y --auto-remove curl; \
    fi && \
    if [ -n "${PKG_SHA}" ] && [ "${PKG_SHA}" != "PLACEHOLDER" ]; then \
        echo "${PKG_SHA}  /tmp/aql.deb" | sha256sum -c -; \
    fi && \
    apt-get install -y --no-install-recommends /tmp/aql.deb && \
    rm -f /tmp/aql.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

CMD ["aql"]

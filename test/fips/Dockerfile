FROM redhat/ubi9:9.6

ARG READLINE_VERSION=8.2
ARG READLINE_SHA256=3feb7171f16a84ee82ca18a36d7b9be109a52c04f492a053331d7d1095007c35
ARG LIBYAML_VERSION=0.2.5
ARG LIBYAML_SHA256=c642ae9b75fee120b2d96c712538bd2cf283228d2337df2cf2988e3c02678ef4

RUN dnf -y install gcc make autoconf automake libtool openssl openssl-devel zlib-devel ncurses-devel \
      findutils diffutils which tar gzip \
 && dnf -y install "https://dl.rockylinux.org/vault/rocky/9.6/AppStream/$(uname -m)/os/Packages/f/flex-2.6.4-9.el9.$(uname -m).rpm" \
 && dnf clean all

RUN cd /tmp \
 && curl -fsSL --retry 5 "https://ftp.gnu.org/gnu/readline/readline-${READLINE_VERSION}.tar.gz" -o readline.tgz \
 && echo "${READLINE_SHA256}  readline.tgz" | sha256sum -c - \
 && tar -xzf readline.tgz \
 && cd "readline-${READLINE_VERSION}" \
 && ./configure --prefix=/usr --libdir=/usr/lib64 CFLAGS=-fPIC \
 && make -j"$(nproc)" SHLIB_LIBS="-lncurses -ltinfo" \
 && make install \
 && cd /tmp \
 && curl -fsSL --retry 5 "https://github.com/yaml/libyaml/releases/download/${LIBYAML_VERSION}/yaml-${LIBYAML_VERSION}.tar.gz" -o yaml.tgz \
 && echo "${LIBYAML_SHA256}  yaml.tgz" | sha256sum -c - \
 && tar -xzf yaml.tgz \
 && cd "yaml-${LIBYAML_VERSION}" \
 && ./configure --prefix=/usr --libdir=/usr/lib64 --enable-static \
 && make -j"$(nproc)" \
 && make install \
 && rm -rf /tmp/*

#!/usr/bin/env bash
set -xeuo pipefail

export FPM_VERSION="1.17.0"

BUILD_DEPS_REDHAT="readline which autoconf libtool openssl-devel zlib-devel"
BUILD_DEPS_AMAZON="readline which autoconf libtool readline-devel flex openssl-devel zlib-devel"
BUILD_DEPS_UBUNTU="libreadline8 libreadline-dev flex autoconf libtool libyaml-dev"
BUILD_DEPS_DEBIAN="libreadline8 libreadline-dev flex autoconf libtool libyaml-dev"

function build_libyaml_static() {
	local LIBYAML_VERSION="0.2.5"
	pushd /tmp
	curl -sL "https://github.com/yaml/libyaml/releases/download/${LIBYAML_VERSION}/yaml-${LIBYAML_VERSION}.tar.gz" | tar xz
	cd "yaml-${LIBYAML_VERSION}"
	./configure --prefix=/usr --enable-static
	make -j"$(nproc)"
	make install
	popd
	rm -rf "/tmp/yaml-${LIBYAML_VERSION}"
}

function install_deps_debian11() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_DEBIAN ruby-rubygems make rpm git curl binutils \
		python3 python3-pip rsync libssl-dev lzma lzma-dev libffi-dev build-essential ruby-dev
	gem install fpm -v "$FPM_VERSION"
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_debian12() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_DEBIAN ruby-rubygems make rpm git curl binutils \
		python3 python3-pip rsync libssl-dev lzma lzma-dev libffi-dev build-essential ruby-dev
	gem install fpm -v "$FPM_VERSION"
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_debian13() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_DEBIAN gcc-13 g++-13 ruby-rubygems make rpm git curl binutils \
		python3 python3-pip rsync libssl-dev lzma liblzma-dev libffi-dev ruby-dev
	update-alternatives --install /usr/bin/gcc gcc "$(which gcc-13)" 10
	update-alternatives --install /usr/bin/g++ g++ "$(which g++-13)" 10
	gem install fpm -v "$FPM_VERSION"
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_ubuntu20.04() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_UBUNTU ruby make rpm git curl binutils \
		python3 python3-pip rsync libssl-dev lzma lzma-dev libffi-dev build-essential ruby-dev
	gem install fpm -v "$FPM_VERSION"
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_ubuntu22.04() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_UBUNTU ruby-rubygems make rpm git curl binutils \
		python3 python3-pip rsync libssl-dev lzma lzma-dev libffi-dev build-essential ruby-dev
	gem install fpm -v "$FPM_VERSION"
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_ubuntu24.04() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_UBUNTU ruby-rubygems make rpm git curl binutils \
		python3 python3-pip rsync libssl-dev lzma lzma-dev libffi-dev build-essential ruby-dev
	gem install fpm -v "$FPM_VERSION"
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_el8() {
	dnf clean all
	dnf -y update
	dnf module enable -y ruby:2.7
	yum install -y "https://download.rockylinux.org/pub/rocky/8.10/AppStream/$(uname -m)/os/Packages/f/flex-2.6.1-9.el8.$(uname -m).rpm"
	yum install -y "https://download.rockylinux.org/pub/rocky/8.10/Devel/$(uname -m)/os/Packages/r/readline-devel-7.0-10.el8.$(uname -m).rpm"
	dnf -y install $BUILD_DEPS_REDHAT gcc-c++ ruby ruby-devel rpm-build make git python3 python3-pip rsync wget
	build_libyaml_static
	gem install --no-document fpm -v "$FPM_VERSION"
	dnf clean all
}

function install_deps_el9() {
	dnf clean all
	dnf -y update
	yum install -y "https://dl.rockylinux.org/vault/rocky/9.6/AppStream/$(uname -m)/os/Packages/f/flex-2.6.4-9.el9.$(uname -m).rpm"
	yum install -y "https://dl.rockylinux.org/vault/rocky/9.6/devel/$(uname -m)/os/Packages/r/readline-devel-8.1-4.el9.$(uname -m).rpm"
	dnf -y install $BUILD_DEPS_REDHAT ruby ruby-devel rpmdevtools make git python3 python3-pip rsync
	build_libyaml_static
	gem install fpm -v "$FPM_VERSION"
	dnf clean all
}

function install_deps_el10() {
	dnf clean all
	dnf -y update
	yum install -y "https://dl.rockylinux.org/vault/rocky/10.0/AppStream/$(uname -m)/os/Packages/f/flex-2.6.4-19.el10.$(uname -m).rpm"
	yum install -y "https://dl.rockylinux.org/vault/rocky/10.0/devel/$(uname -m)/os/Packages/r/readline-devel-8.2-11.el10.$(uname -m).rpm"
	dnf -y install $BUILD_DEPS_REDHAT ruby ruby-devel rpmdevtools make git python3 python3-pip rsync
	build_libyaml_static
	gem install fpm -v "$FPM_VERSION"
	dnf clean all
}

function install_deps_amzn2023() {
	dnf clean all
	dnf -y update
	dnf -y install $BUILD_DEPS_AMAZON ruby ruby-devel rpmdevtools make git python3 python3-pip rsync
	build_libyaml_static
	gem install fpm -v "$FPM_VERSION"
	dnf clean all
}

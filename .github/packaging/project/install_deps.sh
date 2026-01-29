#!/usr/bin/env bash
set -xeuo pipefail

BUILD_DEPS_REDHAT="readline which autoconf libtool openssl-devel zlib-devel libyaml-devel"
BUILD_DEPS_AMAZON="readline which autoconf libtool readline-devel flex openssl-devel zlib-devel libyaml-devel"
BUILD_DEPS_UBUNTU="libreadline8 libreadline-dev flex autoconf libtool libyaml-dev"
BUILD_DEPS_DEBIAN="libreadline8 libreadline-dev flex autoconf libtool libyaml-dev"

function install_deps_debian11() {
  rm -rf /var/lib/apt/lists/*
  apt-get clean
  apt-get update -o Acquire::Retries=5
  apt -y install $BUILD_DEPS_DEBIAN ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl1.1 libssl-dev lzma \
                 lzma-dev libffi-dev
  gem install fpm -v 1.17.0
  rm -rf /var/lib/apt/lists/*
}

function install_deps_debian12() {
  rm -rf /var/lib/apt/lists/*
  apt-get clean
  apt-get update -o Acquire::Retries=5
  apt -y install $BUILD_DEPS_DEBIAN ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev lzma \
                 lzma-dev libffi-dev
  gem install fpm -v 1.17.0
  rm -rf /var/lib/apt/lists/*
}


function install_deps_debian13() {
  rm -rf /var/lib/apt/lists/*
  apt-get clean
  apt-get update -o Acquire::Retries=5
  apt -y install $BUILD_DEPS_DEBIAN gcc-13 g++-13 ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev lzma \
                 liblzma-dev libffi-dev
  update-alternatives --install /usr/bin/gcc gcc $(which gcc-13) 10
  update-alternatives --install /usr/bin/g++ g++ $(which g++-13) 10
  gem install fpm -v 1.17.0
  rm -rf /var/lib/apt/lists/*
}

function install_deps_ubuntu20.04() {
  rm -rf /var/lib/apt/lists/*
  apt-get clean
  apt-get update -o Acquire::Retries=5
  apt -y install $BUILD_DEPS_UBUNTU ruby make rpm git snapd curl binutils python3 python3-pip rsync libssl1.1 libssl-dev \
                 lzma lzma-dev libffi-dev

  gem install fpm -v 1.17.0
  rm -rf /var/lib/apt/lists/*
}

function install_deps_ubuntu22.04() {
  rm -rf /var/lib/apt/lists/*
  apt-get clean
  apt-get update -o Acquire::Retries=5
  apt -y install $BUILD_DEPS_UBUNTU ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev \
               lzma lzma-dev libffi-dev
  gem install fpm -v 1.17.0
  rm -rf /var/lib/apt/lists/*
}

function install_deps_ubuntu24.04() {
  rm -rf /var/lib/apt/lists/*
  apt-get clean
  apt-get update -o Acquire::Retries=5
  apt -y install $BUILD_DEPS_UBUNTU ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev \
               lzma lzma-dev libffi-dev
  gem install fpm -v 1.17.0
  rm -rf /var/lib/apt/lists/*
}


function install_deps_el8() {
  dnf clean all
  dnf -y update
  dnf module enable -y ruby:2.7
  yum install -y "https://download.rockylinux.org/pub/rocky/8.10/AppStream/$(uname -m)/os/Packages/f/flex-2.6.1-9.el8.$(uname -m).rpm"
  yum install -y "https://download.rockylinux.org/pub/rocky/8.10/Devel/$(uname -m)/os/Packages/r/readline-devel-7.0-10.el8.$(uname -m).rpm"
  dnf -y install $BUILD_DEPS_REDHAT gcc-c++ ruby rpm-build make git python3 python3-pip rsync wget
  gem install fpm -v 1.17.0
  dnf clean all
}

function install_deps_el9() {
  dnf clean all
  dnf -y update
  #todo redhat el9 does not have flex or readline-devel available in the yum repos
  yum install -y "https://dl.rockylinux.org/vault/rocky/9.6/AppStream/$(uname -m)/os/Packages/f/flex-2.6.4-9.el9.$(uname -m).rpm"
  yum install -y "https://dl.rockylinux.org/vault/rocky/9.6/devel/$(uname -m)/os/Packages/r/readline-devel-8.1-4.el9.$(uname -m).rpm"
  dnf -y install $BUILD_DEPS_REDHAT ruby rpmdevtools make git python3 python3-pip rsync
  gem install fpm -v 1.17.0
  dnf clean all
}

function install_deps_el10() {
  dnf clean all
  dnf -y update
  yum install -y "https://dl.rockylinux.org/vault/rocky/10.0/AppStream/$(uname -m)/os/Packages/f/flex-2.6.4-19.el10.$(uname -m).rpm"
  yum install -y "https://dl.rockylinux.org/vault/rocky/10.0/devel/$(uname -m)/os/Packages/r/readline-devel-8.2-11.el10.$(uname -m).rpm"
  dnf -y install $BUILD_DEPS_REDHAT ruby rpmdevtools make git python3 python3-pip rsync
  gem install fpm -v 1.17.0
  dnf clean all
}


function install_deps_amzn2023() {
  dnf clean all
  dnf -y update
  dnf -y install $BUILD_DEPS_AMAZON ruby rpmdevtools make git python3 python3-pip rsync
  gem install fpm -v 1.17.0
  dnf clean all
}

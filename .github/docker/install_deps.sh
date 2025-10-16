#!/usr/bin/env bash

BUILD_DEPS_REDHAT="readline which autoconf libtool" #readline-devel flex
BUILD_DEPS_AMAZON="readline which autoconf libtool readline-devel flex"
BUILD_DEPS_UBUNTU="libreadline8 libreadline-dev flex autoconf libtool"
BUILD_DEPS_DEBIAN="libreadline8 libreadline-dev flex autoconf libtool"
EL10_READLINE_VERSION="8.2"
EL8_READLINE_VERSION="7.0"
FLEX_VERSION="2.6.4"

function install_deps_debian11() {
  apt -y install $BUILD_DEPS_DEBIAN ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl1.1 libssl-dev lzma \
                 lzma-dev  libffi-dev
  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  asdf install python 3.10.18
  asdf set python 3.10.18
  /root/.asdf/installs/python/3.10.18/bin/python3 -m pip install pipenv
  install /root/.asdf/installs/python/3.10.18/bin/pipenv /usr/local/bin/pipenv
  gem install fpm
}

function install_deps_debian12() {
  apt -y install $BUILD_DEPS_DEBIAN ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev lzma \
                 lzma-dev libffi-dev
  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install --break-system-packages pipenv
  gem install fpm
}


function install_deps_debian13() {
  apt -y install $BUILD_DEPS_DEBIAN gcc-13 g++-13 ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev lzma \
                 liblzma-dev libffi-dev
  update-alternatives --install /usr/bin/gcc gcc $(which gcc-13) 10
  update-alternatives --install /usr/bin/g++ g++ $(which g++-13) 10
  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install --break-system-packages pipenv
  gem install fpm
}

function install_deps_ubuntu20.04() {
  apt -y install $BUILD_DEPS_UBUNTU ruby make rpm git snapd curl binutils python3 python3-pip rsync libssl1.1 libssl-dev \
                 lzma lzma-dev libffi-dev
  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install pipenv
  gem install fpm
}

function install_deps_ubuntu22.04() {
  apt -y install $BUILD_DEPS_UBUNTU ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev \
               lzma lzma-dev libffi-dev
  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install pipenv
  gem install fpm
}

function install_deps_ubuntu24.04() {
  apt -y install $BUILD_DEPS_UBUNTU ruby-rubygems make rpm git snapd curl binutils python3 python3-pip rsync libssl3 libssl-dev \
               lzma lzma-dev libffi-dev
  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install --break-system-packages pipenv
  gem install fpm
}

function install_deps_redhat-el8() {
  # install fpm
  dnf module enable -y ruby:2.7
  dnf -y install ruby ruby-devel redhat-rpm-config rubygems rpm-build make git
  gem install --no-document fpm

  #todo redhat el8 does not have flex or readline-devel available in the yum repos
  yum install -y https://rpmfind.net/linux/almalinux/8.10/BaseOS/$(uname -m)/os/Packages/readline-devel-7.0-10.el8.$(uname -m).rpm
  yum install -y https://rpmfind.net/linux/almalinux/8.10/AppStream/$(uname -m)/os/Packages/flex-2.6.1-9.el8.$(uname -m).rpm
  dnf -y install $BUILD_DEPS_REDHAT gcc-c++ ruby rpm-build make git python3 python3-pip rsync

  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  dnf install -y gcc make automake zlib zlib-devel libffi-devel openssl-devel bzip2-devel xz-devel xz xz-libs \
                      sqlite sqlite-devel sqlite-libs
  echo "python 3.10.18" > /.tool-versions
  echo "python 3.10.18" > /root/.tool-versions

  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec python -m pip install pipenv
  gem install fpm
}

function install_deps_redhat-el8() {
  # install fpm
##  dnf module enable -y ruby:2.7
##  dnf -y install ruby ruby-devel redhat-rpm-config rubygems rpm-build make git
##  gem install --no-document fpm

  # install readline-devel from source
##  wget http://ftp.gnu.org/gnu/readline/readline-${EL8_READLINE_VERSION}.tar.gz && \
##  tar -xzf readline-${EL8_READLINE_VERSION}.tar.gz && \
##  cd readline-${EL8_READLINE_VERSION} && \
##  ./configure --prefix=/usr --includedir=/usr/include --libdir=/usr/lib && \
##  make SHLIB_LIBS="-lncurses -ltinfo" && \
##  make install
  # install flex / lex
##  wget https://github.com/westes/flex/releases/download/v${FLEX_VERSION}/flex-${FLEX_VERSION}.tar.gz && \
## ## tar -xvzf flex-${FLEX_VERSION}.tar.gz && \
##  cd flex-${FLEX_VERSION} && \
##  ./configure --prefix=/usr && \
##  make -j$(nproc) && \
##  make install && \
##  ln -s /usr/bin/flex /usr/bin/lex

##  dnf config-manager --set-enabled ubi-8-codeready-builder 
##  dnf -y install $BUILD_DEPS_REDHAT gcc-c++ ruby rpm-build make git python3 python3-pip rsync libedit-devel ncurses-devel

##  if [ "$(uname -m)" = "x86_64" ]; then
##      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
##      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
##  elif [ "$(uname -m)" = "aarch64" ]; then
##      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
##      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
##  else
##      echo "unknown arch $(uname -m)"
##      exit 1
##  fi
##  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
##  install /root/go/bin/asdf /usr/local/bin/asdf
##  asdf plugin add python https://github.com/asdf-community/asdf-python.git
##  dnf install -y gcc make automake zlib zlib-devel libffi-devel openssl-devel bzip2-devel xz-devel xz xz-libs \
##                      sqlite sqlite-devel sqlite-libs
##  echo "python 3.10.18" > /.tool-versions
##  echo "python 3.10.18" > /root/.tool-versions

##  asdf install python 3.10.18
##  asdf set python 3.10.18
##  asdf exec python -m pip install pipenv
##}

function install_deps_redhat-el9() {
  #todo redhat el9 does not have flex or readline-devel available in the yum repos
  yum install -y https://rpmfind.net/linux/centos-stream/9-stream/AppStream/$(uname -m)/os/Packages/readline-devel-8.1-4.el9."$(uname -m)".rpm
  yum install -y https://rpmfind.net/linux/centos-stream/9-stream/AppStream/$(uname -m)/os/Packages/flex-2.6.4-9.el9."$(uname -m)".rpm
  dnf -y install $BUILD_DEPS_REDHAT ruby rpmdevtools make git python3 python3-pip rsync

  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  dnf install -y gcc g++ make automake zlib zlib-devel libffi-devel openssl-devel bzip2-devel xz-devel xz xz-libs \
                      sqlite sqlite-devel sqlite-libs
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install pipenv
  gem install fpm
}

function install_deps_redhat-el10() {
  # install readline-devel from source
  wget http://ftp.gnu.org/gnu/readline/readline-${EL10_READLINE_VERSION}.tar.gz && \
  tar -xzf readline-${EL10_READLINE_VERSION}.tar.gz && \
  cd readline-${EL10_READLINE_VERSION} && \
  ./configure --prefix=/usr --includedir=/usr/include --libdir=/usr/lib && \
  make SHLIB_LIBS="-lncurses -ltinfo" && \
  make install
  # install flex / lex
  wget https://github.com/westes/flex/releases/download/v${FLEX_VERSION}/flex-${FLEX_VERSION}.tar.gz && \
  tar -xvzf flex-${FLEX_VERSION}.tar.gz && \
  cd flex-${FLEX_VERSION} && \
  ./configure --prefix=/usr && \
  make -j$(nproc) && \
  make install && \
  ln -s /usr/bin/flex /usr/bin/lex

  dnf -y install $BUILD_DEPS_REDHAT ruby rpmdevtools git python3 python3-pip rsync \

  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  dnf install -y gcc g++ make automake zlib zlib-devel libffi-devel openssl-devel bzip2-devel xz-devel xz xz-libs \
                      sqlite sqlite-devel sqlite-libs
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install pipenv
  gem install fpm
}

function install_deps_amazon-2023() {

  dnf -y install $BUILD_DEPS_AMAZON ruby rpmdevtools make git python3 python3-pip rsync

  if [ "$(uname -m)" = "x86_64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-amd64.tar.gz -o /tmp/go1.24.6.linux-amd64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-amd64.tar.gz -C /opt/golang
  elif [ "$(uname -m)" = "aarch64" ]; then
      curl -L https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go1.24.6.linux-arm64.tar.gz
      mkdir -p /opt/golang && tar -zxvf /tmp/go1.24.6.linux-arm64.tar.gz -C /opt/golang
  else
      echo "unknown arch $(uname -m)"
      exit 1
  fi
  /opt/golang/go/bin/go install github.com/asdf-vm/asdf/cmd/asdf@v0.18.0
  install /root/go/bin/asdf /usr/local/bin/asdf
  asdf plugin add python https://github.com/asdf-community/asdf-python.git
  dnf install -y gcc g++ make automake zlib zlib-devel libffi-devel openssl-devel bzip2-devel xz-devel xz xz-libs \
                      sqlite sqlite-devel sqlite-libs
  asdf install python 3.10.18
  asdf set python 3.10.18
  asdf exec pip install pipenv
  gem install fpm
}

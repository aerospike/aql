#!/usr/bin/env bash
set -xeuo pipefail

function build_packages() {
	if [ "${ENV_DISTRO:-}" = "" ]; then
		echo "ENV_DISTRO is not set"
		return
	fi
	GIT_DIR=$(git rev-parse --show-toplevel)
	PKG_DIR="$GIT_DIR/pkg"

	# build
	cd "$GIT_DIR" || exit 1
	make

	echo "build_package.sh version: $(git describe --tags --always --abbrev=9)"
	VERSION=${PKG_VERSION:-$(git describe --tags --always --abbrev=9)}
	export VERSION

	# package
	cd "$PKG_DIR" || exit 1
	echo "building package for $BUILD_DISTRO"

	if [[ $ENV_DISTRO == *"ubuntu"* ]]; then
		make deb
	elif [[ $ENV_DISTRO == *"debian"* ]]; then
		make deb
	elif [[ $ENV_DISTRO == *"el"* ]]; then
		make rpm
	elif [[ $ENV_DISTRO == *"amzn"* ]]; then
		make rpm
	else
		make tar
	fi

	mkdir -p /tmp/output/"$ENV_DISTRO"
	cp -a "$PKG_DIR"/target/* /tmp/output/"$ENV_DISTRO"
}

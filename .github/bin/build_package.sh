#!/usr/bin/env bash
set -xeuo pipefail

function assert_dynamic_deps() {
	local allowed="libc.so.6 libm.so.6 libpthread.so.0 libdl.so.2 librt.so.1
		libgcc_s.so.1 libz.so.1 libtinfo.so.6 ld-linux-x86-64.so.2 ld-linux-aarch64.so.1"
	if [ "$ENV_DISTRO" = "el8" ] || [ "$ENV_DISTRO" = "debian11" ]; then
		allowed+=" libssl.so.1.1 libcrypto.so.1.1"
	else
		allowed+=" libssl.so.3 libcrypto.so.3"
	fi

	local lib fail=0
	local bin="target/$(uname -s)-$(uname -m)/bin/aql"
	local needed
	needed=$(readelf -d "$bin" | awk '/\(NEEDED\)/ { gsub(/[][]/, "", $NF); print $NF }')
	echo "aql DT_NEEDED:" $needed
	for lib in $needed; do
		if ! printf '%s\n' $allowed | grep -qxF "$lib"; then
			echo "aql has unexpected dynamic dependency $lib; link it statically or add it to the allowlist and the package depends" >&2
			fail=1
		fi
	done
	return $fail
}

function build_packages() {
	if [ "${ENV_DISTRO:-}" = "" ]; then
		echo "ENV_DISTRO is not set" >&2
		return 1
	fi
	GIT_DIR=$(git rev-parse --show-toplevel)
	PKG_DIR="$GIT_DIR/pkg"

	# build (AQL_VERSION must match packaged SemVer; git describe lags until tag exists)
	cd "$GIT_DIR" || exit 1
	VERSION=${PKG_VERSION:-$(git describe --tags --always --abbrev=9)}
	export VERSION
	export AQL_VERSION="${VERSION}"
	make clean
	make

	assert_dynamic_deps

	echo "build_package.sh version: ${VERSION}"

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

# Variables required for this Makefile
PKG_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
TOP_DIR = $(PKG_DIR)/../target/"$(shell uname -s)-$(shell uname -m)"
BUILD_DIR = $(PKG_DIR)/build
TARGET_DIR = $(PKG_DIR)/target
CONFIG_DIR = /etc/aerospike
DOC_DIR = /opt/aerospike/doc/$(PACKAGE_NAME)
MAC_DOC_DIR = /usr/local/aerospike/doc/$(PACKAGE_NAME)
# Package variables
PACKAGE_NAME = aql
BUILD_DISTRO ?= $(shell $(PKG_DIR)/../.github/bin/os_version.sh)
NAME = "aerospike-$(PACKAGE_NAME)"
MAINTAINER = "Aerospike"
VERSION ?= $(shell git describe --tags --always --abbrev=9)
DESCRIPTION = "AQL, a tool for interacting with the Aerospike database."
LICENSE = "Apache License 2.0"
URL = "https://github.com/aerospike/$(PACKAGE_NAME)"
VENDOR = "Aerospike, Inc."
ARCH = $(shell uname -m)
# Package names never carry the "rcN" pre-release tag: rcN becomes the package
# iteration, so 9.2.10-rc2 packages as version 9.2.10 iteration 2.
PKG_RELEASE_SH = $(PKG_DIR)/../.github/bin/pkg_release.sh
PKG_VERSION := $(shell $(PKG_RELEASE_SH) '$(VERSION)' version)
ITERATION := $(or $(ITERATION),$(shell $(PKG_RELEASE_SH) '$(VERSION)' iteration))
# For names with no iteration field of their own (.tar, macOS .pkg).
RELEASE := $(PKG_VERSION)-$(ITERATION)
RPM_VERSION := $(subst -,_,$(PKG_VERSION))
# rpm Release / deb revision. The deb revision embeds the distro per Debian
# convention (e.g. 8.0.4-1debian12), like the rpm dist tag.
DEB_ITERATION := $(ITERATION)$(BUILD_DISTRO)
# pkgbuild --version rejects hyphens and underscores; collapse both to dots
# (8.0.4-1 -> 8.0.4.1), and defensively normalize any underscore-form
# inputs (env overrides) too. This is the version *inside* the pkg only;
# the file name below keeps the canonical hyphenated RELEASE.
MAC_VERSION := $(subst _,.,$(subst -,.,$(RELEASE)))
MAC_ROOT = $(BUILD_DIR)/mac-root
PKG_ID = com.aerospike.aql
# macOS artifact file name, lower case and '-' separated so a Homebrew cask can
# interpolate it (<arch> is `uname -m`, the token a cask's `arch` stanza emits):
#
#     aerospike-aql-<version>-<iteration>-macos<major>-<arch>.pkg
#
# macos<major> is the generation the artifact was BUILT on, not a minimum it
# supports; it keeps one release's per-runner artifacts distinct. MAC_PLATFORM
# is `:=` so check-version's guard tests the token that lands in the name.
MAC_PLATFORM := $(shell $(PKG_DIR)/../.github/bin/os_version.sh)
MAC_PKG = $(TARGET_DIR)/aerospike-aql-$(RELEASE)-$(MAC_PLATFORM)-$(ARCH).pkg


.PHONY: all
all: deb rpm tar

.PHONY: deb
deb: prep
	fpm --force \
		--config-files $(CONFIG_DIR) \
		--input-type dir \
		--output-type deb \
		--chdir $(BUILD_DIR)/ \
		--name $(NAME) \
		--version $(PKG_VERSION) \
		--iteration $(DEB_ITERATION) \
		--maintainer $(MAINTAINER) \
		--description $(DESCRIPTION) \
		--license $(LICENSE) \
		--url $(URL) \
		--vendor $(VENDOR) \
		--after-install $(PKG_DIR)/postinst.sh \
		--after-upgrade $(PKG_DIR)/postinst.sh \
		--package $(TARGET_DIR)/$(NAME)_$(PKG_VERSION)-$(DEB_ITERATION)_$(ARCH).deb

.PHONY: rpm
rpm: prep
	fpm --force \
		--config-files $(CONFIG_DIR) \
		--input-type dir \
		--output-type rpm \
		--chdir $(BUILD_DIR)/ \
		--name $(NAME) \
		--version $(RPM_VERSION) \
		--iteration $(ITERATION) \
		--maintainer $(MAINTAINER) \
		--depends "openssl-libs" \
		--depends "ncurses-libs" \
		--description $(DESCRIPTION) \
		--license $(LICENSE) \
		--url $(URL) \
		--vendor $(VENDOR) \
		--after-install $(PKG_DIR)/postinst.sh \
		--after-upgrade $(PKG_DIR)/postinst.sh \
		--package $(TARGET_DIR)/$(NAME)-$(RPM_VERSION)-$(ITERATION).$(BUILD_DISTRO).$(ARCH).rpm

.PHONY: tar
tar: prep
	fpm --force \
		--config-files $(CONFIG_DIR) \
		--input-type dir \
		--output-type tar \
		--chdir $(BUILD_DIR)/ \
		--name $(NAME) \
		--version $(PKG_VERSION) \
		--maintainer $(MAINTAINER) \
		--description $(DESCRIPTION) \
		--license $(LICENSE) \
		--url $(URL) \
		--vendor $(VENDOR) \
		--package $(TARGET_DIR)/$(NAME)-$(RELEASE)-$(BUILD_DISTRO)-$(ARCH).tar

# PKG_VERSION, ITERATION and MAC_PLATFORM all come from $(shell ...), which
# yields the empty string on failure without failing the build; unguarded, an
# empty one ships aerospike-aql_-<distro>_<arch>.deb past every later check.
.PHONY: check-version
check-version:
	@test -n '$(VERSION)' && test '$(VERSION)' != 'unknown' \
		|| { echo "ERROR: VERSION is '$(VERSION)' -- git describe failed and none was passed; refusing to build an unversioned package" >&2; exit 1; }
	@test -n '$(PKG_VERSION)' \
		|| { echo "ERROR: empty package version -- $(PKG_RELEASE_SH) failed for VERSION='$(VERSION)'" >&2; exit 1; }
	@test -n '$(ITERATION)' \
		|| { echo "ERROR: empty package iteration -- $(PKG_RELEASE_SH) failed for VERSION='$(VERSION)'" >&2; exit 1; }

.PHONY: prep
prep: check-version
	install -d $(TARGET_DIR)
	install -d $(BUILD_DIR)/$(CONFIG_DIR)
	install -d $(BUILD_DIR)/opt/aerospike/bin
	install -pm 755 $(TOP_DIR)/bin/aql $(BUILD_DIR)/opt/aerospike/bin
	install -d $(BUILD_DIR)/usr/bin
	ln -sf /opt/aerospike/bin/aql $(BUILD_DIR)/usr/bin/aql
	install -d $(BUILD_DIR)$(DOC_DIR)
	install -pm 644 $(PKG_DIR)/astools.conf.sample $(BUILD_DIR)$(DOC_DIR)/astools.conf.sample



# Staging for macOS installer: install to /usr/local/aerospike/bin with symlink at /usr/local/bin.
.PHONY: prep-mac
prep-mac: check-version
	@echo '$(MAC_PLATFORM)' | grep -qE '^macos[0-9]+$$' \
		|| { echo "ERROR: macOS platform token is '$(MAC_PLATFORM)', not macos<major> -- .github/bin/os_version.sh failed; refusing to build a misnamed pkg" >&2; exit 1; }
	install -d $(TARGET_DIR)
	rm -rf $(MAC_ROOT)
	install -d $(MAC_ROOT)/usr/local/aerospike/bin
	install -d $(MAC_ROOT)/usr/local/bin
	install -pm 755 $(TOP_DIR)/bin/aql $(MAC_ROOT)/usr/local/aerospike/bin/aql
	ln -sf ../aerospike/bin/aql $(MAC_ROOT)/usr/local/bin/aql
	install -d $(MAC_ROOT)$(MAC_DOC_DIR)
	install -pm 644 $(PKG_DIR)/astools.conf.sample $(MAC_ROOT)$(MAC_DOC_DIR)/astools.conf.sample

.PHONY: osx-pkg
osx-pkg: prep-mac
	pkgbuild \
		--root $(MAC_ROOT) \
		--identifier $(PKG_ID) \
		--version $(MAC_VERSION) \
		--install-location / \
		--scripts $(PKG_DIR)/mac-scripts \
		$(MAC_PKG)

.PHONY: clean
clean:
	rm -rf $(TARGET_DIR)
	rm -rf $(BUILD_DIR)/opt
	rm -rf $(BUILD_DIR)/var
	rm -rf $(MAC_ROOT)

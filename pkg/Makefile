# Variables required for this Makefile
PKG_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
TOP_DIR = $(PKG_DIR)/../target/"$(shell uname -s)-$(shell uname -m)"
BUILD_DIR = $(PKG_DIR)/build
TARGET_DIR = $(PKG_DIR)/target
CONFIG_DIR = /etc/aerospike
# Package variables
BUILD_DISTRO ?= $(shell $(PKG_DIR)/../.github/bin/os_version.sh)
NAME = "aerospike-$(PACKAGE_NAME)"
MAINTAINER = "Aerospike"
VERSION ?= $(shell git describe --tags --always --abbrev=9)
DESCRIPTION = "AQL, a tool for interacting with the Aerospike database."
LICENSE = "Apache License 2.0"
URL = "https://github.com/aerospike/$(PACKAGE_NAME)"
VENDOR = "Aerospike, Inc."
ARCH = $(shell uname -m)
RPM_VERSION := $(subst -,_,$(VERSION))
MAC_VERSION := $(RPM_VERSION)
MAC_ROOT = $(BUILD_DIR)/mac-root
PKG_ID = com.aerospike.aql
MAC_PKG = $(TARGET_DIR)/aerospike-aql-$(MAC_VERSION)-$(ARCH).pkg


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
		--version $(VERSION) \
		--maintainer $(MAINTAINER) \
		--description $(DESCRIPTION) \
		--license $(LICENSE) \
		--url $(URL) \
		--vendor $(VENDOR) \
		--package $(TARGET_DIR)/$(NAME)_$(VERSION)_$(BUILD_DISTRO)_$(ARCH).deb

.PHONY: rpm
rpm: prep
	fpm --force \
		--config-files $(CONFIG_DIR) \
		--input-type dir \
		--output-type rpm \
		--chdir $(BUILD_DIR)/ \
		--name $(NAME) \
		--version $(VERSION) \
		--maintainer $(MAINTAINER) \
		--description $(DESCRIPTION) \
		--license $(LICENSE) \
		--url $(URL) \
		--vendor $(VENDOR) \
		--package $(TARGET_DIR)/$(NAME)-$(VERSION).$(BUILD_DISTRO).$(ARCH).rpm

.PHONY: tar
tar: prep
	fpm --force \
		--config-files $(CONFIG_DIR) \
		--input-type dir \
		--output-type tar \
		--chdir $(BUILD_DIR)/ \
		--name $(NAME) \
		--version $(VERSION) \
		--maintainer $(MAINTAINER) \
		--description $(DESCRIPTION) \
		--license $(LICENSE) \
		--url $(URL) \
		--vendor $(VENDOR) \
		--package $(TARGET_DIR)/$(NAME)-$(BUILD_DISTRO)-$(VERSION)-$(ARCH).tar

.PHONY: prep
prep:
	install -d $(TARGET_DIR)
	install -d $(BUILD_DIR)/$(CONFIG_DIR)
	install -d $(BUILD_DIR)/opt/aerospike/bin
	install -pm 755 $(TOP_DIR)/bin/aql $(BUILD_DIR)/opt/aerospike/bin
	install -d $(BUILD_DIR)/usr/bin
	ln -sf /opt/aerospike/bin/aql $(BUILD_DIR)/usr/bin/aql



# Staging for macOS installer: install to /usr/local/aerospike/bin with symlink at /usr/local/bin.
.PHONY: prep-mac
prep-mac:
	install -d $(TARGET_DIR)
	rm -rf $(MAC_ROOT)
	install -d $(MAC_ROOT)/usr/local/aerospike/bin
	install -d $(MAC_ROOT)/usr/local/bin
	install -pm 755 $(TOP_DIR)/bin/aql $(MAC_ROOT)/usr/local/aerospike/bin/aql
	ln -sf ../aerospike/bin/aql $(MAC_ROOT)/usr/local/bin/aql

.PHONY: osx-pkg
osx-pkg: prep-mac
	pkgbuild \
		--root $(MAC_ROOT) \
		--identifier $(PKG_ID) \
		--version $(MAC_VERSION) \
		--install-location / \
		$(MAC_PKG)

.PHONY: clean
clean:
	rm -rf $(TARGET_DIR)
	rm -rf $(BUILD_DIR)/opt
	rm -rf $(BUILD_DIR)/var
	rm -rf $(MAC_ROOT)

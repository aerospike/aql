# Variables required for this Makefile
PKG_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
TOP_DIR = $(PKG_DIR)/../target/"$(shell uname -s)-$(shell uname -m)"
BUILD_DIR = $(PKG_DIR)/build
TARGET_DIR = $(PKG_DIR)/target
# aql doesn't support configs yet but fpm complains without this
CONFIG_DIR = /etc/aerospike

# Package variables
NAME = "aql"
VERSION = $(shell git describe --tags --always)
MAINTAINER = "Aerospike"
DESCRIPTION = "AQL, a tool for interacting with the Aerospike database."
LICENSE = "Apache License 2.0"
URL = "https://github.com/aerospike/aql"
VENDOR = "Aerospike, Inc."
ARCH = $(shell uname -m)


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
		--package $(TARGET_DIR)/aql_$(VERSION)_$(BUILD_DISTRO)_$(ARCH).deb

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
		--package $(TARGET_DIR)/aql-$(VERSION).$(BUILD_DISTRO).$(ARCH).rpm

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
		--package $(TARGET_DIR)/aql-$(BUILD_DISTRO)-$(VERSION)-$(ARCH).tar

.PHONY: prep
prep:
	install -d $(TARGET_DIR)
	install -d $(BUILD_DIR)/$(CONFIG_DIR)
	install -d $(BUILD_DIR)/opt/aerospike/bin
	install -pm 755 $(TOP_DIR)/bin/aql $(BUILD_DIR)/opt/aerospike/bin
	install -d $(BUILD_DIR)/usr/bin
	ln -sf /opt/aerospike/bin/aql $(BUILD_DIR)/usr/bin/aql

.PHONY: clean
clean:
	rm -rf $(TARGET_DIR)
	rm -rf $(BUILD_DIR)/opt
	rm -rf $(BUILD_DIR)/var
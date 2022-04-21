include project/build.makefile

# ifndef CLIENTREPO
#   $(error Missing required variable: CLIENTREPO. Please define the variable and try again.)
# endif

# CLIENT_PATH = $(CLIENTREPO)
CLIENT_PATH = ./c-client
CLIENTREPO = $(CLIENT_PATH)
JANSSON_PATH = ./jansson
TOML_PATH = ./toml

DIR_INCLUDE = ../include $(CLIENT_PATH)/src/include
DIR_INCLUDE += $(CLIENTREPO)/modules/common/src/include
DIR_INCLUDE += $(CLIENTREPO)/modules/mod-lua/src/include
DIR_INCLUDE += $(CLIENTREPO)/modules/base/src/include
INCLUDES = $(DIR_INCLUDE:%=-I%) 

CFLAGS = -std=gnu99 -g -O0 -fno-common -fno-strict-aliasing -fPIC -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
CFLAGS += $(INCLUDES) -I$(JANSSON_PATH)/src -I$(TOML_PATH)/

ifeq ($(OS),Darwin)
  CFLAGS += -I/usr/local/include -D_DARWIN_UNLIMITED_SELECT
  ifneq ($(wildcard /usr/local/opt/openssl/include),)
    CFLAGS += -I/usr/local/opt/openssl/include
    ifeq ($(OPENSSL_STATIC_PATH),)
      LDFLAGS += -L/usr/local/opt/openssl/lib
    endif
  endif

else
  CFLAGS += -rdynamic
endif

ifeq ($(OS),Darwin)
  LIBRARIES += $(CLIENT_PATH)/$(TARGET_LIB)/libaerospike.a
  LIBRARIES += $(JANSSON_PATH)/src/.libs/libjansson.a
  LIBRARIES += $(TOML_PATH)/libtoml.a
else
  LIBRARIES += -L$(CLIENT_PATH)/$(TARGET_LIB) -Wl,-l,:libaerospike.a
  LIBRARIES += -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a
  LIBRARIES += -L$(TOML_PATH) -Wl,-l,:libtoml.a
endif

LIBRARIES += -L/usr/local/lib
ifneq ($(OPENSSL_STATIC_PATH),)
  LIBRARIES += $(OPENSSL_STATIC_PATH)/libssl.a
  LIBRARIES += $(OPENSSL_STATIC_PATH)/libcrypto.a
else
  LDFLAGS += -L/usr/local/opt/openssl/lib
  LIBRARIES += -lssl
  LIBRARIES += -lcrypto
endif

# Use the Lua submodule?  [By default, yes.]
USE_LUAMOD = 1

# Use LuaJIT instead of Lua?  [By default, no.]
USE_LUAJIT = 0

# Permit easy overriding of the default.
ifeq ($(USE_LUAJIT),1)
  USE_LUAMOD = 0
endif

ifeq ($(and $(USE_LUAMOD:0=),$(USE_LUAJIT:0=)),1)
  $(error Only at most one of USE_LUAMOD or USE_LUAJIT may be enabled (i.e., set to 1.))
endif

ifeq ($(USE_LUAJIT),1)
  ifeq ($(OS),Darwin)
    LDFLAGS += -pagezero_size 10000 -image_base 100000000
  endif
else
  ifeq ($(USE_LUAMOD),0)
    # Find where the Lua development package is installed in the build environment.
    ifeq ($(OS),Darwin)
      LUA_LIB = -L/usr/local/lib
    else
      INCLUDE_LUA_5_1 = /usr/include/lua5.1
      ifneq ($(wildcard $(INCLUDE_LUA_5_1)),)
	LUA_SUFFIX = 5.1
      endif
    endif
    LUA_LIB += -llua$(LUA_SUFFIX)
  endif
endif

LIBRARIES += $(LUA_LIB) -lpthread -lm -lreadline
ifneq ($(OS),Darwin)
  LIBRARIES += -lhistory -lrt -ldl -lz
else
  LIBRARIES += -lz
endif

# Set the AQL version from the latest Git tag.
CFLAGS += -DAQL_VERSION=\"$(shell git describe)\"

##
## MAIN
##

.DEFAULT_GOAL := all

all: toml jansson c-client aql

LEXER_SRC = sql-lexer.c
.SECONDARY: $(LEXER_SRC)

OBJECTS = 
OBJECTS += main.o
OBJECTS += asql.o
OBJECTS += $(LEXER_SRC:.c=.o)
OBJECTS += asql_admin.o
OBJECTS += asql_explain.o
OBJECTS += asql_info.o
OBJECTS += asql_key.o
OBJECTS += asql_parser.o
OBJECTS += asql_print.o
OBJECTS += asql_tokenizer.o
OBJECTS += asql_query.o
OBJECTS += asql_scan.o
OBJECTS += asql_truncate.o
OBJECTS += asql_value.o
OBJECTS += asql_conf.o
OBJECTS += json.o
OBJECTS += renderer/json_renderer.o
OBJECTS += renderer/table.o
OBJECTS += renderer/no_renderer.o
OBJECTS += renderer/raw_renderer.o
aql: $(call objects, $(OBJECTS)) | $(TARGET_BIN)
	$(call executable, $(empty), $(empty), $(empty), $(LDFLAGS), $(LIBRARIES))
	mkdir -p $(TARGET_BIN)/

.PHONY: c-client
c-client: $(CLIENT_PATH)/$(TARGET_LIB)/libaerospike.a

$(CLIENT_PATH)/$(TARGET_LIB)/libaerospike.a:
	$(MAKE) -C c-client


.PHONY: jansson
jansson: $(JANSSON_PATH)/src/.libs/libjansson.a 

$(JANSSON_PATH)/src/.libs/libjansson.a: $(JANSSON_PATH)/Makefile
	$(MAKE) -C jansson

$(JANSSON_PATH)/Makefile: $(JANSSON_PATH)/configure
	cd jansson && ./configure

$(JANSSON_PATH)/configure:
	cd jansson && autoreconf -i


.PHONY: toml
toml: $(TOML_PATH)/libtoml.a $(TOML_PATH)/toml.o

$(TOML_PATH)/libtoml.a $(TOML_PATH)/toml.o:
	$(MAKE) -C toml


install:
	if [ ! -e /opt/aerospike/bin ]; then mkdir -p /opt/aerospike/bin; fi
	cp -p $(TARGET_BIN)/aql /opt/aerospike/bin/.
	if [ -f /usr/bin/aql ]; then rm /usr/bin/aql; fi
	ln -s /opt/aerospike/bin/aql /usr/bin/aql

tags etags:
	etags `find . $(JANSSON_PATH) -name "*.[chl]"`

cleanmodules:
	if [ -e '$(JANSSON_PATH)/Makefile' ]; then \
		$(MAKE) -C jansson clean; \
		$(MAKE) -C jansson distclean; \
	fi; \
  $(MAKE) -C c-client clean

.PHONY: cleanall
cleanall: clean cleanmodules

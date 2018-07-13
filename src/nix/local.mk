programs += nix

nix_DIR := $(d)

nix_SOURCES := $(wildcard $(d)/*.cc) $(wildcard src/linenoise/*.cpp)

nix_LIBS = libexpr libmain libstore libutil libformat

nix_LDFLAGS = -pthread -laws-cpp-sdk-sqs -laws-cpp-sdk-core

$(eval $(call install-symlink, nix, $(bindir)/nix-hash))

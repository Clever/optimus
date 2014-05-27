SHELL := /bin/bash
PKG := github.com/azylman/getl
SOURCES := $(shell ls sources)
SINKS := $(shell ls sinks)
SUBPKG_NAMES := $(addprefix sources/, $(SOURCES)) $(addprefix sinks/, $(SINKS)) transformer transforms
SUBPKGS = $(addprefix $(PKG)/, $(SUBPKG_NAMES))
PKGS = $(PKG) $(SUBPKGS)

.PHONY: test golint README

test: docs $(PKGS)

golint:
	@go get github.com/golang/lint/golint

README.md: *.go
	@go get github.com/robertkrimen/godocdown/godocdown
	@godocdown $(PKG) > README.md

$(PKGS): golint README
	@go get -d -t $@
	@gofmt -w=true $(GOPATH)/src/$@*/**.go
ifneq ($(NOLINT),1)
	@echo "LINTING..."
	@PATH=$(PATH):$(GOPATH)/bin golint $(GOPATH)/src/$@*/**.go
	@echo ""
endif
ifeq ($(COVERAGE),1)
	@go test -cover -coverprofile=$(GOPATH)/src/$@/c.out $@ -test.v
	@go tool cover -html=$(GOPATH)/src/$@/c.out
else
	@echo "TESTING..."
	@go test $@ -test.v
endif

docs: $(addsuffix /README.md, $(SUBPKG_NAMES)) README.md
%/README.md: PATH := $(PATH):$(GOPATH)/bin
%/README.md: %/*.go
	@go get github.com/robertkrimen/godocdown/godocdown
	@godocdown $(PKG)/$(shell dirname $@) > $@

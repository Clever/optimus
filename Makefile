SHELL := /bin/bash
PKG := github.com/azylman/getl
SUBPKG_NAMES := $(addprefix sources/, $(shell ls sources)) $(addprefix sinks/, $(shell ls sinks)) transformer transforms
SUBPKGS = $(addprefix $(PKG)/, $(SUBPKG_NAMES))
PKGS = $(PKG) $(SUBPKGS)

.PHONY: test golint README

test: $(PKGS)

golint:
	@go get github.com/golang/lint/golint

README.md: *.go
	@go get github.com/robertkrimen/godocdown/godocdown
	@godocdown $(PKG) > README.md
README: README.md

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

SHELL := /bin/bash
PKG := gopkg.in/Clever/gearman.v1
SUBPKGSREL := $(shell ls -d */)
SUBPKGS := $(addprefix $(PKG)/, $(SUBPKGSREL))
READMES := $(addsuffix README.md, $(SUBPKGSREL))
PKGS = $(PKG) $(SUBPKGS)

.PHONY: test golint README

test: $(PKGS) docs

golint:
	@go get github.com/golang/lint/golint

$(PKGS): golint docs
	@go get -d -t $@
	@gofmt -w=true $(GOPATH)/src/$@/*.go
ifneq ($(NOLINT),1)
	@echo "LINTING..."
	@PATH=$(PATH):$(GOPATH)/bin golint $(GOPATH)/src/$@/*.go
	@echo ""
endif
ifeq ($(COVERAGE),1)
	@go test -cover -coverprofile=$(GOPATH)/src/$@/c.out $@ -test.v
	@go tool cover -html=$(GOPATH)/src/$@/c.out
else
	@echo "TESTING..."
	@go test $@ -test.v
	@echo ""
endif

docs: $(READMES) README.md
README.md: *.go
	@go get github.com/robertkrimen/godocdown/godocdown
	godocdown $(PKG) > $@
%/README.md: PATH := $(PATH):$(GOPATH)/bin
%/README.md: %/*.go
	@go get github.com/robertkrimen/godocdown/godocdown
	godocdown $(PKG)/$(shell dirname $@) > $@

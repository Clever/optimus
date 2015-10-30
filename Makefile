SHELL := /bin/bash
PKG := gopkg.in/Clever/optimus.v3
SOURCES := $(shell ls sources)
SINKS := $(shell ls sinks)
SUBPKG_NAMES := $(addprefix sources/, $(SOURCES)) $(addprefix sinks/, $(SINKS)) transformer transforms
SUBPKGS = $(addprefix $(PKG)/, $(SUBPKG_NAMES))
PKGS = $(PKG) $(SUBPKGS)

.PHONY: test docs $(PKGS)

test: docs transformer/gen.go $(PKGS)

$(GOPATH)/bin/golint:
	@go get github.com/golang/lint/golint

$(GOPATH)/bin/godocdown:
	@go get github.com/robertkrimen/godocdown/godocdown

README.md: $(GOPATH)/bin/godocdown *.go
	@$(GOPATH)/bin/godocdown -template=.godocdown.template $(PKG) > README.md

$(PKGS): $(GOPATH)/bin/golint docs
	@go get -d -t $@
	@gofmt -w=true $(GOPATH)/src/$@*/**.go
ifneq ($(NOLINT),1)
	@echo "LINTING..."
	@$(GOPATH)/bin/golint $(GOPATH)/src/$@*/**.go
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

docs: $(addsuffix /README.md, $(SUBPKG_NAMES)) README.md
%/README.md: %/*.go $(GOPATH)/bin/godocdown
	@$(GOPATH)/bin/godocdown $(PKG)/$(shell dirname $@) > $@

transformer/gen.go: transforms/*.go cmd/transformer_generate/*.go
	go run $(GOPATH)/src/$(PKG)/cmd/transformer_generate/*.go

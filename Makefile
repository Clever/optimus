include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
PKG := gopkg.in/Clever/optimus.v3
PKGS := $(shell go list $(PKG)/... | grep -v /vendor)
# NOTE: We have a poorly named type that we choose to not fix as it would break backwards compatibility.
# In 4.0, this type will be renamed and all packages will be tested strictly.
LAX_PKGS := $(addprefix $(PKG),/sources/error /transformer)
STRICT_PKGS := $(filter-out $(LAX_PKGS),$(PKGS))

.PHONY: test docs $(PKGS)
$(eval $(call golang-version-check,1.6))

all: test

test: $(STRICT_PKGS) $(LAX_PKGS)
$(STRICT_PKGS): golang-test-all-strict-deps
	go get -t $@
	$(call golang-test-all-strict,$@)
$(LAX_PKGS): golang-test-all-deps
	go get -t $@
	$(call golang-test-all,$@)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))

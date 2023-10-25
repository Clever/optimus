include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
PKG := github.com/Clever/optimus/v4
PKGS := $(shell go list $(PKG)/... | grep -v /vendor)
# NOTE: We have a poorly named type that we choose to not fix as it would break backwards compatibility.
# In 5.0, this type will be renamed and all packages will be tested strictly.
LAX_PKGS := $(addprefix $(PKG),/sources/error /transformer)
STRICT_PKGS := $(filter-out $(LAX_PKGS),$(PKGS))

.PHONY: test docs install_deps $(PKGS)
$(eval $(call golang-version-check,1.21))

all: test

test: install_deps $(STRICT_PKGS) $(LAX_PKGS)
$(STRICT_PKGS): golang-test-all-strict-deps
	$(call golang-test-all-strict,$@)
$(LAX_PKGS): golang-test-all-deps
	$(call golang-test-all,$@)


install_deps:
	go mod vendor

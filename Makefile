include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
PKG := gopkg.in/Clever/optimus.v3
PKGS := $(shell go list ./... | grep -v /vendor)

.PHONY: test docs $(PKGS)
$(eval $(call golang-version-check,1.5))

all: test

test: $(PKGS)
$(PKGS): golang-test-all-strict-deps
	$(call golang-test-all-strict,$@)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))

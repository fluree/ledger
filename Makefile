RELEASE_BUCKET ?= fluree-releases-public
MINIMUM_JAVA_VERSION ?= 11
JAVA_VERSION_FOR_RELEASE_BUILDS := $(MINIMUM_VERSION)

VERSION := $(shell clojure -M:meta version)

MAJOR_VERSION := $(shell echo $(VERSION) | cut -d '.' -f1)
MINOR_VERSION := $(shell echo $(VERSION) | cut -d '.' -f2)

ifneq ($(strip $(shell which shasum)),)
  SHACMD := shasum -a 256
else
  SHACMD := sha256sum
endif

.PHONY: deps test eastwood ci jar uberjar stage-release run check-release-jdk-version prep-release print-version release release-stable release-latest release-version-latest docker-image install clean

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

DESTDIR ?= /usr/local

RELEASE_TARGETS := build/release-staging build/fluree-ledger.standalone.jar \
                   build/fluree_start.sh build/logback.xml \
                   build/fluree_sample.properties build/LICENSE \
                   build/CHANGELOG.md

build/fluree-$(VERSION).zip: $(RELEASE_TARGETS)
	cd build && zip -r fluree-$(VERSION).zip * -x 'data/' 'data/**' 'release-staging/' 'release-staging/**'

build/fluree-$(VERSION).zip.sha256: build/fluree-$(VERSION).zip
	cd $(@D) && $(SHACMD) $(<F) > $(@F)

stage-release: $(RELEASE_TARGETS)

print-version:
	@echo $(VERSION)

run: $(RELEASE_TARGETS)
	cd build && ./fluree_start.sh

check-release-jdk-version:
	resources/fluree_start.sh java_version $(JAVA_VERSION_FOR_RELEASE_BUILDS)

prep-release: check-release-jdk-version clean build/fluree-$(VERSION).zip build/fluree-$(VERSION).zip.sha256 build/release-staging
	cp build/fluree-$(VERSION).zip* build/release-staging/

release: prep-release
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

release-stable: prep-release
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-stable.zip
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-latest.zip
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-$(MAJOR_VERSION).$(MINOR_VERSION)-latest.zip
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

release-latest: prep-release
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-latest.zip
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-stable.zip
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-$(MAJOR_VERSION).$(MINOR_VERSION)-latest.zip
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

release-version-latest: prep-release
	cp build/release-staging/fluree-$(VERSION).zip build/release-staging/fluree-$(MAJOR_VERSION).$(MINOR_VERSION)-latest.zip
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

deps: deps.edn
	clojure -Stree

package-lock.json: package.json
	npm install && touch package-lock.json

node_modules/%: package.json package-lock.json
	npm install && touch $@

resources/adminUI: | node_modules/@fluree/admin-ui/build
	ln -nsf ../$| $@

build:
	mkdir -p build

build/release-staging:
	mkdir -p build/release-staging

build/fluree-ledger.standalone.jar: target/fluree-ledger.standalone.jar | build
	cp $< $(@D)/

build/LICENSE: LICENSE | build
	cp $< $(@D)/

build/CHANGELOG.md: CHANGELOG.md | build
	cp $< $(@D)/

build/%: resources/% | build
	cp $< $(@D)/

target/fluree-ledger.jar: resources/adminUI $(SOURCES) $(RESOURCES)
	clojure -X:jar

jar: target/fluree-ledger.jar

unit-test:
	clojure -X:test :excludes '[:integration]'

integration-test:
	clojure -X:test :includes '[:integration]'

test:
	clojure -X:test

eastwood:
	clojure -M:test:eastwood

ci: test eastwood

target/fluree-ledger.standalone.jar: resources/adminUI deps.edn $(SOURCES) $(RESOURCES)
	clojure -X:uberjar

uberjar: target/fluree-ledger.standalone.jar

ifneq ($(strip $(shell which git)),)
  ifeq ($(strip $(shell git status --porcelain)),)
    git_tag := $(shell git rev-parse HEAD)
  endif
endif

docker-image:
	docker buildx build --load -t fluree/ledger:$(VERSION) .
ifdef git_tag
	docker buildx build --load -t fluree/ledger:$(git_tag) .
endif

docker-push:
	docker buildx build --platform 'linux/amd64,linux/arm64' --push -t fluree/ledger:$(VERSION) .
ifdef git_tag
	docker buildx build --platform 'linux/amd64,linux/arm64' --push -t fluree/ledger:$(git_tag) .
endif

docker-push-latest:
	docker buildx build --platform 'linux/amd64,linux/arm64' --push -t fluree/ledger:latest .

$(DESTDIR)/etc/fluree.properties: resources/fluree_sample.properties
	install -d $(@D)
	install -m 0644 $^ $@

$(DESTDIR)/etc/fluree-logback.xml: resources/logback.xml
	install -d $(@D)
	install -m 0644 $^ $@

$(DESTDIR)/share/java/fluree-ledger.standalone.jar: build/fluree-ledger.standalone.jar
	install -d $(@D)
	install -m 0644 $^ $@

$(DESTDIR)/bin/fluree: resources/fluree_start.sh
	install -d $(@D)
	install $^ $@

install: $(DESTDIR)/bin/fluree $(DESTDIR)/share/java/fluree-ledger.standalone.jar | $(DESTDIR)/etc/fluree.properties $(DESTDIR)/etc/fluree-logback.xml

clean:
	@# only delete contents of build dir if full delete fails (e.g. b/c we're mounting it as a Docker volume)
	rm -rf build 2>/dev/null || rm -rf build/*
	rm -rf target
	rm -rf classes
	rm -f resources/adminUI
	rm -rf node_modules
	rm -f pom.xml
	rm -rf scanning_results

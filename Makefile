RELEASE_BUCKET ?= fluree-releases-public
MINIMUM_JAVA_VERSION ?= 11
JAVA_VERSION_FOR_RELEASE_BUILDS := $(MINIMUM_VERSION)

VERSION := $(shell clojure -M:meta version)
VERSION ?= SNAPSHOT

MAJOR_VERSION := $(shell echo $(VERSION) | cut -d '.' -f1)
MINOR_VERSION := $(shell echo $(VERSION) | cut -d '.' -f2)

.PHONY: deps test jar uberjar stage-release run check-release-jdk-version prep-release release release-stable release-latest release-version-latest docker-image install clean

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

DESTDIR ?= /usr/local

print-version:
	@echo $(VERSION)

build/fluree-$(VERSION).zip: stage-release
	cd build && zip -r fluree-$(VERSION).zip * -x 'data/' 'data/**' 'release-staging/' 'release-staging/**'

stage-release: build/release-staging build/fluree-ledger.standalone.jar build/fluree_start.sh build/logback.xml build/fluree_sample.properties build/LICENSE build/CHANGELOG.md

run: stage-release
	cd build && ./fluree_start.sh

check-release-jdk-version:
	resources/fluree_start.sh java_version $(JAVA_VERSION_FOR_RELEASE_BUILDS)

prep-release: check-release-jdk-version clean build/fluree-$(VERSION).zip build/release-staging
	cp build/fluree-$(VERSION).zip build/release-staging/

release: prep-release
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

release-stable: prep-release
	cp build/fluree-$(VERSION).zip build/release-staging/fluree-stable.zip
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

release-latest: prep-release
	cp build/fluree-$(VERSION).zip build/release-staging/fluree-latest.zip
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

release-version-latest: prep-release
	cp build/fluree-$(VERSION).zip build/release-staging/fluree-$(MAJOR_VERSION).$(MINOR_VERSION)-latest.zip
	aws s3 sync build/release-staging/ s3://$(RELEASE_BUCKET)/ --size-only --cache-control max-age=300 --acl public-read --profile fluree

deps: deps.edn
	clojure -Stree

package-lock.json: package.json
	npm install

node_modules: package.json package-lock.json
	npm install && touch $@

resources/adminUI: node_modules
	ln -nsf ../node_modules/@fluree/admin-ui/build $@

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

target/fluree-ledger.jar: pom.xml resources/adminUI $(SOURCES) $(RESOURCES)
	clojure -X:jar

jar: target/fluree-ledger.jar

pom.xml: deps.edn
	clojure -Spom

test:
	clojure -M:test

target/fluree-ledger.standalone.jar: pom.xml resources/adminUI $(SOURCES) $(RESOURCES)
	clojure -X:uberjar

uberjar: target/fluree-ledger.standalone.jar

ifneq ($(strip $(shell which git)),)
  ifeq ($(strip $(shell git status --porcelain)),)
    git_tag := $(shell git rev-parse HEAD)
  endif
endif

docker-image:
	docker build --build-arg DEPS_KEY=$(DEPS_KEY) --build-arg DEPS_SECRET=$(DEPS_SECRET) -t fluree/ledger:$(VERSION) .
ifdef git_tag
	docker tag fluree/ledger:$(VERSION) fluree/ledger:$(git_tag)
endif

docker-push: docker-image
	docker push fluree/ledger:$(VERSION)
ifdef git_tag
	docker push fluree/ledger:$(git_tag)
endif

docker-push-latest: docker-push
	docker tag fluree/ledger:$(VERSION) fluree/ledger:latest
	docker push fluree/ledger:latest

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
	rm -rf build
	rm -rf target
	rm -rf resources/adminUI
	rm -rf node_modules

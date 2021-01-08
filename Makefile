RELEASE_BUCKET ?= fluree-releases-public

VERSION := $(shell mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)
VERSION ?= SNAPSHOT

MAJOR_VERSION := $(shell echo $(VERSION) | cut -d '.' -f1)
MINOR_VERSION := $(shell echo $(VERSION) | cut -d '.' -f2)

.PHONY: deps test jar uberjar stage-release run prep-release release release-stable release-latest release-version-latest docker-image clean

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

build/fluree-$(VERSION).zip: stage-release
	cd build && zip -r fluree-$(VERSION).zip * -x 'data/' 'data/**'

stage-release: build/release-staging build/fluree-ledger.standalone.jar build/fluree_start.sh build/logback.xml build/fluree_sample.properties build/LICENSE build/CHANGELOG.md

run: stage-release
	build/fluree_start.sh

prep-release: build/fluree-$(VERSION).zip build/release-staging
	rm -rf build/release-staging/*
	cp $< build/release-staging/

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

build:
	mkdir -p build

build/release-staging:
	mkdir -p build/release-staging

build/fluree-ledger.standalone.jar: target/fluree-ledger.standalone.jar
	cp $< build/

build/fluree_start.sh: resources/fluree_start.sh
	cp $< build/

build/fluree_sample.properties: resources/fluree_sample.properties
	cp $< build/

build/LICENSE: LICENSE
	cp $< build/

build/CHANGELOG.md: CHANGELOG.md
	cp $< build/

build/logback.xml: dev/logback.xml
	cp $< build/

target/fluree-ledger.jar: pom.xml $(SOURCES) $(RESOURCES)
	clojure -X:jar

jar: target/fluree-ledger.jar

pom.xml: deps.edn
	clojure -Spom

test:
	clojure -M:test

target/fluree-ledger.standalone.jar: pom.xml $(SOURCES) $(RESOURCES)
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

clean:
	rm -rf build
	rm -rf target

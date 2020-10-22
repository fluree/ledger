# Fluree ledger

## Usage

Download the latest or stable release from https://flur.ee/getstarted/ and
follow the instructions there.

### Docker

A Docker image is available from the Docker Hub. It is named `fluree/ledger`.

### Kubernetes

Kubernetes support is in the `k8s/` subdirectory of this project. You can
deploy Fluree to a k8s cluster running Kubernetes 1.18+ with a default storage
class configured by applying the YAML files in the `k8s/` directory:

`kubectl apply -f k8s`

## Development

### Contributing

All contributors must complete a [Contributor License Agreement](https://cla-assistant.io/fluree/).

### Prerequisites

1. Install clojure tools-deps (version 1.10.1.697 or later).
    1. macOS: `brew install clojure/tools/clojure`
    1. Arch Linux: `pacman -S clojure`

### Tests

You can run the integration tests like this:

`make test`

### Building

1. Install maven
    1. macOS: `brew install maven`
    1. Arch Linux: `pacman -S maven`
1. Set the version you want in `pom.xml`
1. Run `make`

This will generate `build/fluree-$(VERSION).zip`.

Its contents will also be left in the `build/` directory for manual testing
(e.g. `cd build && ./fluree_start.sh`).

### Releasing

By default releases will be uploaded to the `fluree-releases-public` S3 bucket.
You can override this with `make release RELEASE_BUCKET=fluree-releases-test`
(or any bucket you have write access to).

You'll need the AWS command line tools installed and a `fluree` profile configured in your
`~/.aws/` directory.

#### Release types

1. Full version string
    1. Run `make release`
1. Stable
    1. Run `make release-stable`
1. Latest
    1. Run `make release-latest`
1. Major.Minor-latest
    1. Run `make release-version-latest`

Note that every release type will also upload the full version string if necessary, so
there is no need to do e.g. `make release && make release-stable` to get that.

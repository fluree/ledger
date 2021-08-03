# Fluree ledger

## Usage

Fluree ledger requires Java 11 or later.

Download the latest or stable release from https://flur.ee/getstarted/ and
follow the instructions there.

### Docker

A Docker image is available from the Docker Hub. It is named `fluree/ledger`.

### Kubernetes

Kubernetes support is in the `k8s/` subdirectory of this project. You can
deploy Fluree to a k8s cluster running Kubernetes 1.18+ with a default storage
class configured by applying the YAML files in the `k8s/` directory:

`kubectl apply -f k8s`

## License 

Fluree Ledger source code is licensed under AGPL. What if AGPL is a problem for your organization?

[fluree/ledger](https://github.com/fluree/ledger) runs as an independent service, so there really is no need to include
or modify Fluree Ledger code, which is what would trigger AGPL restrictions. This differs from
[fluree/db](https://github.com/fluree/db) which is designed to be included with your code as a library - and for that reason is licensed under EPL.

If you want to run fluree/ledger under a different license, that is easy to solve by using a precompiled
version of the code (AGPL only covers the source code distribution). Easiest way to do that is [download
it here](https://flur.ee/getstarted/), and it will be covered by our standard
 [customer license agreement](https://flur.ee/sales-documents/) terms.

Does your organization not allow free software at all? Contact us for a commercial license.

## Development

### Contributing

All contributors must complete a [Contributor License Agreement](https://cla-assistant.io/fluree/).

### Prerequisites

1. Install clojure tools-deps (version 1.10.2.774 or later).
    1. macOS: `brew install clojure/tools/clojure`
    1. Arch Linux: `pacman -S clojure`
1. Install NodeJS & npm
    1. macOS: `brew install node`
   
#### Using local adminUI

If you're hacking on the adminUI, open `package.json` and change the version
that `@fluree/admin-ui` is pointed at to `file:../admin-ui` (or wherever your
local copy of fluree-admin-ui is cloned).

Then do `make clean` followed by `make run`.

While actively hacking on the adminUI, it probably makes more sense to run it
separately with its change-watcher turned on. Then you can run the above once
you're ready to test integration of the production build.

### Tests

You can run the integration tests like this:

`make test`

### Building

1. Set the version you want in `deps.edn` (in the `:mvn/version` alias)
1. Run `make`

This will generate `build/fluree-$(VERSION).zip`.

Its contents will also be left in the `build/` directory for manual testing
(e.g. `cd build && ./fluree_start.sh`).

### System-wide installation

`make install` will install fluree to `/usr/local` (by default) and you can then run
`fluree` to start it up. Config files `fluree.properties` and `fluree-logback.xml` will
be copied to `/usr/local/etc` if they don't already exist. You can modify them to your
needs.

If you want to install somewhere other than `/usr/local` you can set the `DESTDIR` variable
like this: `make install DESTDIR=/other/path`. Fluree will then be installed to `/other/path/bin`,
`/other/path/etc`, & `/other/path/share/java`.

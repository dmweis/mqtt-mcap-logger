DEB_BUILD_PATH ?= target/debian/mqtt-mcap-logger*.deb

.PHONY: build
build:
	cargo build --release
	cargo deb --no-build

.PHONE: install
install: build
	sudo dpkg -i $(ARM_BUILD_PATH)

.PHONY: install-dependencies
install-dependencies:
	cargo install cargo-deb

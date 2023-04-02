DEB_BUILD_PATH ?= target/debian/mqtt-mcap-logger*.deb

.PHONY: build
build:
	cargo build --release
	cargo deb --no-build

.PHONE: install
install: build
	sudo dpkg -i $(DEB_BUILD_PATH)

.PHONY: install-dependencies
install-dependencies:
	sudo apt update && sudo apt install -y liblzma-dev dpkg dpkg-dev
	cargo install cargo-deb

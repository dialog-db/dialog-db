{
  description = "Staging";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    { nixpkgs
    , flake-utils
    , rust-overlay
    , ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain =
          toolchain:
          let
            rustToolchain = pkgs.rust-bin.${toolchain}.latest.default.override {
              targets = [
                "wasm32-wasip1"
                "wasm32-unknown-unknown"
                "aarch64-unknown-linux-gnu"
              ];
            };
          in
          if builtins.hasAttr toolchain pkgs.rust-bin then
            rustToolchain
          else
            throw "Unsupported Rust toolchain: ${toolchain}";

        wasm-bindgen-cli =
          with pkgs;
          rustPlatform.buildRustPackage rec {
            pname = "wasm-bindgen-cli";
            version = "0.2.100";
            buildInputs =
              [ rust-bin.stable.latest.default ]
              ++ lib.optionals stdenv.isDarwin [
                darwin.apple_sdk.frameworks.SystemConfiguration
                darwin.apple_sdk.frameworks.Security
              ];

            src = fetchCrate {
              inherit pname version;
              sha256 = "sha256-3RJzK7mkYFrs7C/WkhW9Rr4LdP5ofb2FdYGz1P7Uxog=";
            };

            cargoHash = "sha256-qsO12332HSjWCVKtf1cUePWWb9IdYUmT+8OPj/XP2WE=";
            useFetchCargoVendor = true;
          };

        common-build-inputs =
          toolchain:
            with pkgs;
            let
              rust-toolchain = rustToolchain toolchain;
            in
            with pkgs;
            [
              binaryen
              gnused
              pkg-config
              protobuf
              rust-toolchain
              trunk
              wasm-bindgen-cli
              wasm-pack
            ]
            ++ lib.optionals stdenv.isDarwin [
              darwin.apple_sdk.frameworks.SystemConfiguration
              darwin.apple_sdk.frameworks.Security
            ];

        common-dev-tools = with pkgs; [
          cargo-nextest
          nodejs
        ];

        interactive-dev-tools =
          with pkgs;
          common-dev-tools
          ++ [
            static-web-server
          ]
          ++ lib.optionals stdenv.isLinux [
            chromium
            chromedriver
          ];
      in
      {
        devShells = {
          default =
            with pkgs;
            mkShell {
              buildInputs = common-build-inputs "stable" ++ interactive-dev-tools;

              shellHook = ''
                export PATH=$PATH:./node_modules/.bin
                export CHROMEDRIVER="${chromedriver}/bin/chromedriver"
                export WASM_BINDGEN_TEST_TIMEOUT=180
              '';
            };
        };

        packages = {
          dialog-artifacts-web =
            let

              rust-toolchain = rustToolchain ("stable");

              rust-platform = pkgs.makeRustPlatform {
                cargo = rust-toolchain;
                rustc = rust-toolchain;
              };
            in
            rust-platform.buildRustPackage {
              name = "dialog-artifacts";
              src = ./.;
              doCheck = false;
              env = {
                RUST_BACKTRACE = "full";
              };
              buildPhase = ''
                # NOTE: wasm-pack currently requires a writable $HOME
                # directory to be set
                # SEE: https://github.com/rustwasm/wasm-pack/issues/1318#issuecomment-1713377536
                export HOME=`pwd`

                wasm-pack build --release --target web --weak-refs -m no-install ./rust/dialog-artifacts
              '';
              installPhase = ''
                mkdir -p $out
                cp -r ./rust/dialog-artifacts/pkg $out/dialog-artifacts
              '';

              nativeBuildInputs = common-build-inputs "stable";
              cargoLock = {
                lockFile = ./Cargo.lock;
              };
            };
        };
      }
    );
}

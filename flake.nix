{
  description = "Staging";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    wrangler-flake.url = "github:ryand56/wrangler";
  };

  outputs =
    { nixpkgs
    , flake-utils
    , rust-overlay
    , wrangler-flake
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
              cargo-nextest
              cloudflare-cli
              esbuild
              gnused
              nodejs
              pkg-config
              protobuf
              rust-toolchain
              trunk
              wasm-bindgen-cli
              wasm-pack
              worker-build
              wrangler-flake.packages.${system}.wrangler
            ]
            ++ lib.optionals stdenv.isDarwin [
              darwin.apple_sdk.frameworks.SystemConfiguration
              darwin.apple_sdk.frameworks.Security
            ];

        interactive-dev-tools =
          with pkgs;
          [
            static-web-server
            leptosfmt
            cargo-generate
          ]
          ++ lib.optionals stdenv.isLinux [
            chromium
            chromedriver
          ];

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

              wasm-pack build --release --scope dialog-db --target web --weak-refs -m no-install ./rust/dialog-artifacts
            '';
            installPhase = ''
              mkdir -p $out/@dialog-db
              cp -r ./rust/dialog-artifacts/pkg $out/@dialog-db/dialog-artifacts
              rm $out/@dialog-db/dialog-artifacts/.gitignore
            '';

            nativeBuildInputs = common-build-inputs "stable";
            cargoLock = {
              lockFile = ./Cargo.lock;
            };
          };


        dialog-artifacts-web-tests = with pkgs;
          buildNpmPackage {
            pname = "dialog-artifacts-web-tests";
            version = "0.1.0";
            src = ./typescript/dialog-artifacts-web-tests/.;
            npmDepsHash = "sha256-o0NiimFWGXf8xQlsmQ+L+B11RqNStu7TVo5iw1GU5sU=";

            buildInputs = [
              dialog-artifacts-web
              dialog-experimental
            ];

            nativeBuildInputs = common-build-inputs "stable" ++ [
              chromium
            ];

            env = {
              CHROME_PATH = "${chromium}/bin/chromium";
            };

            buildPhase = ''
              cp -r ${dialog-artifacts-web}/@dialog-db/dialog-artifacts ./dialog-artifacts
            '';

            checkPhase = ''
              npm test
            '';

            # TODO: Can't seem to get headless tests to run under chroot
            doCheck = false;
          };

        dialog-experimental = with pkgs;
          buildNpmPackage {
            pname = "@dialog-db/experimental";
            version = "0.1.0";

            src = ./typescript/dialog-experimental/.;

            # npmDepsHash = lib.fakeHash;
            npmDepsHash = "sha256-qcnrYVltgUUXWQRFT9TzYfHOcdUswfEI/j6WkZ41HmU=";

            nativeBuildInputs = common-build-inputs "stable" ++ [
              playwright-driver
            ];

            env = {
              PLAYWRIGHT_BROWSERS_PATH = playwright-driver.browsers;
              PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS = true;
              PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = true;
              npm_config_loglevel = "verbose";
            };

            buildPhase = ''
              mkdir -p src/artifacts
              cp -r ${dialog-artifacts-web}/@dialog-db/dialog-artifacts/* src/artifacts/
              npm run build
            '';

            installPhase = ''
              mkdir -p $out/@dialog-db/experimental
              cp -r ./src \
                ./dist \
                ./tsconfig.json \
                ./package.json \
                ./package-lock.json \
                ./web-test-runner.config.mjs \
                ./test $out/@dialog-db/experimental
            '';

            checkPhase = ''
              npm test
            '';

            # TODO: Can't seem to get headless tests to run under chroot
            doCheck = false;
          };

        dialog-remote-cloudflare-worker =
          let
            rust-toolchain = rustToolchain ("stable");

            rust-platform = pkgs.makeRustPlatform {
              cargo = rust-toolchain;
              rustc = rust-toolchain;
            };
          in
          rust-platform.buildRustPackage {
            pname = "dialog-remote-cloudflare-worker";
            version = "0.1.0";
            src = ./.;
            nativeBuildInputs = common-build-inputs "stable";

            buildPhase = ''
              export HOME=`pwd`
              cd rust/dialog-remote
              wrangler build
            '';

            installPhase = ''
              mkdir -p $out/dialog-remote-cloudflare-worker
              cp ./wrangler.deploy.toml $out/dialog-remote-cloudflare-worker/wrangler.toml
              cp -r ./build $out/dialog-remote-cloudflare-worker
            '';

            doCheck = false;

            env = {
              RUST_BACKTRACE = "full";
            };

            cargoLock = {
              lockFile = ./Cargo.lock;
            };
          };

        # dialog-remote-cloudflare-worker = with pkgs; stdenv.mkDerivation
        #   {
        #     pname = "dialog-remote-cloudflare-worker";
        #     version = "0.1.0";
        #     src = ./.;
        #     nativeBuildInputs = common-build-inputs "stable";
        #     buildPhase = ''
        #       export HOME=`pwd`
        #       cd rust/dialog-remote
        #       wrangler build
        #     '';
        #     installPhase = ''
        #       mkdir -p $out/
        #       cp -r ./rust/dialog-remote/build $out/dialog-remote-cloudflare-worker
        #     '';
        #   };

        npm-packages = with pkgs;
          stdenv.mkDerivation {
            pname = "npm-packages";
            version = "0.1.0";
            buildInputs = [
              dialog-artifacts-web
              dialog-experimental
            ];
            src = ./.;
            buildPhase = "";
            installPhase = ''
              mkdir -p $out/@dialog-db
              cp -r ${dialog-artifacts-web}/@dialog-db/dialog-artifacts $out/@dialog-db
              cp -r ${dialog-experimental}/@dialog-db/experimental $out/@dialog-db
            '';
          };

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
                export PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS=1
                export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1;
                export PLAYWRIGHT_BROWSERS_PATH=${playwright-driver.browsers}
              '';
            };
        };

        checks = {
          inherit dialog-experimental dialog-artifacts-web-tests;
        };

        packages =
          {
            inherit dialog-artifacts-web dialog-remote-cloudflare-worker dialog-experimental npm-packages;
          };
      }
    );
}

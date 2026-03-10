{
  description = "Dialog";

  inputs = {
    crane.url = "github:ipetkov/crane";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nix-filter.url = "github:numtide/nix-filter";
  };

  outputs =
    {
      self,
      crane,
      nixpkgs,
      flake-utils,
      rust-overlay,
      nix-filter,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (import rust-overlay)
          ];
        };
        filter = nix-filter.lib;

        commonBuildInputs =
          with pkgs;
          [
            binaryen
            gnused
            pkg-config
            protobuf
            trunk
            wasm-bindgen-cli
            wasm-pack
          ]
          ++ lib.optionals stdenv.isLinux [
            chromium
            chromedriver
          ]
          ++ lib.optionals stdenv.isDarwin [
            apple-sdk
          ];

        # Import rust helpers
        rustHelpers = (
          import ./nix/rust.nix {
            inherit pkgs filter crane;
            buildInputs = commonBuildInputs;
            workspaceRoot = ./.;
          }
        );

        inherit (rustHelpers)
          buildWasmCrate
          buildTestArchive
          cargoChecks
          rustToolchain
          wasm-bindgen-cli
          ;

        developmentBuildInputs =
          with pkgs;
          (
            commonBuildInputs
            ++ [
              nodejs
              cargo-nextest
              rustToolchain
            ]
          );

        developmentEnvVars =
          with pkgs;
          {
            "WASM_BINDGEN_TEST_TIMEOUT" = "180";
          }
          // lib.optionalAttrs stdenv.isLinux {
            "CHROME" = "${chromium}/bin/chromium";
            "CHROMEDRIVER" = "${chromedriver}/bin/chromedriver";
            "CHROME_PATH" = "${chromium}/bin/chromium";
          }
          # Chromium is not packaged for darwin in nixpkgs
          # (https://github.com/NixOS/nixpkgs/issues/247855),
          # so we fall back to the default system Chrome install path.
          // lib.optionalAttrs stdenv.isDarwin {
            "CHROME_PATH" = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
          };

        dialog-artifacts-web = buildWasmCrate {
          pname = "dialog-artifacts";

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
        };

        dialog-experimental =
          with pkgs;
          buildNpmPackage {
            pname = "@dialog-db/experimental";
            version = "0.1.0";

            src = ./typescript/dialog-experimental/.;
            npmDepsHash = "sha256-qcnrYVltgUUXWQRFT9TzYfHOcdUswfEI/j6WkZ41HmU=";

            nativeBuildInputs = developmentBuildInputs;
            env = {
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

            doCheck = false;
          };

        dialog-artifacts-web-tests =
          with pkgs;
          buildNpmPackage {
            pname = "dialog-artifacts-web-tests";
            version = "0.1.0";
            src = ./typescript/dialog-artifacts-web-tests/.;
            npmDepsHash = "sha256-sMaPwgasaObNZPeGGKynj8DL/V5AXNWU82AOBOp530g=";

            buildInputs = [
              dialog-artifacts-web
              dialog-experimental
            ];

            nativeBuildInputs = developmentBuildInputs;

            # Skip Puppeteer's Chrome download during npm ci in the Nix sandbox.
            # At test runtime, CHROME_PATH is provided by developmentEnvVars.
            env = {
              "PUPPETEER_SKIP_DOWNLOAD" = "true";
            };

            buildPhase = ''
              cp -r ${dialog-artifacts-web}/@dialog-db/dialog-artifacts ./dialog-artifacts
            '';

            installPhase = ''
              mkdir -p "$out/"
              cp -r ./* "$out/"
            '';

            doCheck = false;
          };

        dialog-npm-packages =
          with pkgs;
          stdenv.mkDerivation {
            pname = "dialog_npm_packages";
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

        # Import menu helpers (e.g., colorful shell commands)
        menuHelpers = (
          import ./nix/menu.nix {
            inherit pkgs;
          }
        );

        inherit (menuHelpers) makeMenu makeDevShellHook menuTestCommand;

        commands = {
          "bench" = {
            description = "Run all benchmarks";
            command = "cargo bench";
          };

          "lint" = {
            description = "Lint the full source tree";
            command = "nix flake check";
          };

          "test:all" = {
            description = "Run the full test suite (all configurations, grab a coffee)";
            command = ''
              test:native:debug
              test:native:release
              test:web:debug
              test:web:release
              test:native:ucan
              test:web:ucan
              test:cross:integration
              test:npm
            '';

          };

          "test:native:debug" = menuTestCommand {
            description = "Unit and integration tests (${system}, debug)";
            package = "tests-native-debug";
          };

          "test:native:release" = menuTestCommand {
            description = "Unit and integration tests (${system}, release)";
            package = "tests-native-release";
          };

          "test:web:debug" = menuTestCommand {
            description = "Unit and integration tests (wasm32-unknown-unknown, debug)";
            package = "tests-web-debug";
          };

          "test:web:release" = menuTestCommand {
            description = "Unit and integration tests (wasm32-unknown-unknown, release)";
            package = "tests-web-debug";
          };

          "test:native:ucan" = menuTestCommand {
            description = "UCAN-specific tests (${system}, debug)";
            package = "tests-native-ucan";
          };

          "test:web:ucan" = menuTestCommand {
            description = "UCAN-specific tests (wasm32-unknown-unknown, debug)";
            package = "tests-web-ucan";
          };

          "test:cross:integration" = menuTestCommand {
            description = "Cross-target integration tests (${system} + wasm32-unknown-unknown, debug)";
            package = "tests-cross-integration";
          };

          "test:npm" = {
            description = "JavaScript unit tests for NPM packages";
            command = ''
              # Skip Puppeteer's Chrome download during npm ci; tests use
              # the browser specified by CHROME_PATH instead.
              export PUPPETEER_SKIP_DOWNLOAD=true

              nix build .#dialog-artifacts-web-tests
              TEST_DIR=$(mktemp -d);

              cp -r ./result/* "$TEST_DIR"
              chmod -R 755 "$TEST_DIR"
              pushd "$TEST_DIR"

              npm ci
              npm test
            '';
          };
        };

        menu = makeMenu commands;
      in
      {
        test = commonBuildInputs;

        packages = {
          inherit
            dialog-artifacts-web
            dialog-artifacts-web-tests
            dialog-experimental
            dialog-npm-packages
            ;

          tests-native-debug = buildTestArchive {
            name = "native-debug";
            args = "--features s3,s3-list,integration-tests";
          };

          tests-native-release = buildTestArchive {
            name = "native-release";
            args = "--release --features s3,s3-list,integration-tests";
          };

          tests-web-debug = buildTestArchive {
            name = "web-debug";
            target = "wasm32-unknown-unknown";
            args = "--features s3,s3-list";
          };

          tests-web-release = buildTestArchive {
            name = "web-debug";
            target = "wasm32-unknown-unknown";
            args = "--features s3,s3-list --release";
          };

          tests-native-ucan = buildTestArchive {
            name = "native-ucan";
            args = "--features ucan";
          };

          tests-web-ucan = buildTestArchive {
            name = "web-ucan";
            target = "wasm32-unknown-unknown";
            args = "--features ucan";
          };

          tests-cross-integration = buildTestArchive {
            name = "cross-integration";
            args = "--features s3,s3-list,web-integration-tests";
          };
        };

        checks = cargoChecks // {
          # Other checks here...
        };

        devShells = with pkgs; {
          default = mkShell {
            env = developmentEnvVars;
            nativeBuildInputs = menu.commands ++ developmentBuildInputs;
            shellHook = makeDevShellHook menu;
          };
        };
      }
    );

  nixConfig = {
    extra-substituters = [
      "https://tonk-ops.cachix.org"
    ];
    extra-trusted-public-keys = [
      "tonk-ops.cachix.org-1:gMKFoFyM4aGZLazSU7msgKpEa1kEZ9nulJnld8em+1A="
    ];
  };

}

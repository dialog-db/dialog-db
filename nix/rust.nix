# This module contains helpers for building Rust-based artifacts
# It exists because we're using [crane](https://crane.dev) to do
# the building, and correct crane usage is somewhat nuanced compared
# to the built-in Nix tools (such as buildRustPackage). Using the
# helpers here means you can maximize the amount of sharing / re-use
# of dependencies across Rust projects.
{
  pkgs,
  filter,
  crane,
  workspaceRoot,
  buildInputs,
}:

let
  # Cargo dependencies that are Git repositories need to have their
  # expected build hash recorded separately. We make a shared variable so
  # that the same dependencies can be used across all derivations that
  # need them. Crane expects the full git URL as the key.
  cargoGitDependencies = {
    # "git+https://github.com/dialog-db/dialog-db.git?tag=tonk-2026-02-14#9ee48119c262d3cdea87bbd23b75d83dc766a146" =
    #   "sha256-H8a4o7vRRVeXaBLu7KbTCKeLIz1drrbDsEX5WsuX14I=";
  };

  # Filter source to only Rust-relevant files
  # rustSource = craneLib.cleanCargoSource (craneLib.path workspaceRoot);
  rustSource = filter {
    root = workspaceRoot;
    include = [
      ".cargo"
      "Cargo.lock"
      "Cargo.toml"
      "rust-toolchain.toml"
      "rust"
    ];
  };

  rustToolchain = pkgs.rust-bin.fromRustupToolchainFile (workspaceRoot + "/rust-toolchain.toml");
  craneLib = (crane.mkLib pkgs).overrideToolchain (_: rustToolchain);

  wasm-bindgen-cli =
    with pkgs;
    buildWasmBindgenCli rec {
      src = fetchCrate {
        pname = "wasm-bindgen-cli";
        version = "0.2.126";
        hash = "sha256-H6Is3fiZVxZCfOMWK5dWMSrtn50VGv0sfdnsT+cTtyk=";
      };

      cargoDeps = rustPlatform.fetchCargoVendor {
        inherit src;
        inherit (src) pname version;
        hash = "sha256-VucqkXbCi4qtQzY/HrXiDnbSURsagPsdNVMn1Tw3UiY=";
      };
    };

  # Built with crane (rather than rustPlatform.buildRustPackage) so its
  # vendored dependencies are cached in the shared binary cache alongside the
  # rest of the workspace. buildRustPackage's cargoHash drives a vendor-staging
  # fixed-output derivation that fetches crates live from crates.io on every
  # cache miss, which fails intermittently when crates.io rate-limits CI
  # runners (HTTP 403). Crane vendors from the crate's bundled Cargo.lock and
  # the resulting artifacts substitute from cachix like everything else.
  enforce-workspace-deps =
    let
      src = pkgs.fetchCrate {
        pname = "cargo-enforce-shared-workspace-deps";
        version = "0.1.0";
        sha256 = "sha256-XOdKeg9tNt/HT+WO9QKtdX3fUMUssVTlXRV0LOIMMzc=";
      };
    in
    craneLib.buildPackage {
      inherit src;
      pname = "cargo-enforce-shared-workspace-deps";
      version = "0.1.0";
      strictDeps = true;
      cargoVendorDir = craneLib.vendorCargoDeps { inherit src; };
      nativeBuildInputs = [ rustToolchain ];
      doCheck = false;
    };

  nativeBuildInputs = buildInputs ++ [
    rustToolchain
  ];

  # Workspace-wide common attributes
  commonAttributes = {

    src = rustSource;
    strictDeps = true;
    inherit nativeBuildInputs;
    buildInputs =
      with pkgs;
      lib.optionals stdenv.isLinux [
        dbus
      ];

    # Git dependencies with hashes for offline evaluation
    # Crane will automatically find Cargo.lock from src
    outputHashes = cargoGitDependencies;
    doCheck = false;
  };

  # Build native dependencies once for entire workspace
  nativeArtifacts = craneLib.buildDepsOnly (
    commonAttributes
    // {
      pname = "dialog-db-workspace-deps";
    }
  );

  wasmAttributes = commonAttributes // {
    CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
  };

  # wbg-pool is a native host binary (the wasm test runner); its dependency
  # tree (mio, tokio net/process/signal, ...) does not build for wasm32, so
  # keep it out of the wasm dependency build.
  wasmArtifacts = craneLib.buildDepsOnly (
    wasmAttributes
    // {
      pname = "dialog-db-workspace-wasm-deps";
      cargoExtraArgs = "--workspace --exclude wbg-pool";
    }
  );

  # Generic crate builder using crane
  buildCrate =
    attributes:
    craneLib.buildPackage (
      commonAttributes
      // {
        version = "0.1.0";
        cargoArtifacts = nativeArtifacts;
      }
      // attributes
    );

  # The wasm test runner. A native workspace binary that nextest invokes once
  # per test via the wasm32 target runner; it keeps one headless Chrome alive
  # and hands each test a fresh origin. Built from the workspace source so it
  # tracks the same wasm-bindgen pin as the crates under test.
  wbg-pool = buildCrate {
    pname = "wbg-pool";
    cargoExtraArgs = "--locked -p wbg-pool";
    doCheck = false;
  };

  buildWasmCrate =
    attributes:
    craneLib.buildPackage (
      wasmAttributes
      // {
        cargoArtifacts = wasmArtifacts;

        # These *_BIN envvars are an implicit part of the `worker-build` API
        WASM_OPT_BIN = "${pkgs.binaryen}/bin/wasm-opt";
        WASM_BINDGEN_BIN = "${wasm-bindgen-cli}/bin/wasm-bindgen";
        ESBUILD_BIN = "${pkgs.esbuild}/bin/esbuild";
      }
      // attributes
    );

  buildTrunkCrate =
    attributes:
    let
      crateRoot = builtins.dirOf attributes.trunkConfig;
    in
    craneLib.buildTrunkPackage (
      wasmAttributes
      // {
        cargoArtifacts = wasmArtifacts;
        preBuild = ''
          cd ${crateRoot}
        '';
        inherit wasm-bindgen-cli;
      }
      // attributes
    );

  # Build cargo-nextest test archive
  buildTestArchive =
    {
      name,
      args ? "",
      target ? null,
    }:
    let
      isWasm = target == "wasm32-unknown-unknown";

      targetAttributes = if isWasm then wasmAttributes else commonAttributes;

      targetArtifacts = if isWasm then wasmArtifacts else nativeArtifacts;

      # wbg-pool is a native host binary and does not build for wasm32, so keep
      # it out of the wasm test archive.
      excludeArgs = if isWasm then "--workspace --exclude wbg-pool" else "";
    in
    craneLib.mkCargoDerivation (
      targetAttributes
      // {
        pname = "tests-${name}";
        cargoArtifacts = targetArtifacts;

        buildPhaseCargoCommand = ''
          cargo nextest archive \
            ${excludeArgs} \
            ${args} \
            --archive-file ./tests-${name}.tar.zst
        '';

        installPhaseCommand = ''
          mkdir -p $out
          cp ./*.tar.zst $out/
        '';

        doInstallCargoArtifacts = false;
        nativeBuildInputs = (targetAttributes.nativeBuildInputs or [ ]) ++ [ pkgs.cargo-nextest ];
      }
    );

  cargoChecks = {
    clippy = craneLib.cargoClippy (
      commonAttributes
      // {
        pname = "dialog-db-cargo-clippy-check";
        cargoArtifacts = nativeArtifacts;
        cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
      }
    );

    rustfmt = craneLib.cargoFmt {
      src = rustSource;
      pname = "dialog-db-cargo-fmt-check";
    };

    sharedWorkspaceDeps = buildCrate {
      pname = "shared-workspace-deps-check";
      buildPhase = ''
        ${enforce-workspace-deps}/bin/cargo-enforce-shared-workspace-deps
      '';
      installPhase = ''
        touch $out
      '';
    };
  };

in
{
  inherit
    buildCrate
    buildWasmCrate
    buildTrunkCrate
    buildTestArchive
    rustToolchain
    cargoChecks
    wasm-bindgen-cli
    wbg-pool
    ;
}

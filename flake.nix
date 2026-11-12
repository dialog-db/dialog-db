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
          config = {
            allowUnfreePredicate =
              pkg:
              builtins.elem (pkgs.lib.getName pkg) [
                "google-chrome"
              ];
          };
        };

        filter = nix-filter.lib;

        chrome = if pkgs.stdenv.isDarwin then pkgs.google-chrome else pkgs.chromium;
        chromePath = "${chrome}/bin/${chrome.meta.mainProgram}";

        # NOTE: We only need to make use of this on Darwin at this time
        webdriverConfig =
          with pkgs;
          (formats.json { }).generate "webdriver.json" {
            "goog:chromeOptions" = {
              binary = "${chromePath}";
              args = [
                "--no-sandbox"
                "--disable-gpu"
              ];
            };
          };

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
          buildTestArchive
          cargoChecks
          rustToolchain
          wasm-bindgen-cli
          wbg-pool
          wasmCcEnv
          testProfile
          ;

        developmentBuildInputs =
          with pkgs;
          (
            commonBuildInputs
            ++ [
              nodejs
              cargo-nextest
              rustToolchain
              chrome
              chromedriver
              # The wasm32 target runner (see .cargo/config.toml); nextest
              # invokes it once per test against a single shared browser.
              wbg-pool
            ]
            # The cost matrix (rust/dialog-perf) counts each scenario's
            # instructions under callgrind; valgrind has no darwin build.
            ++ lib.optionals stdenv.isLinux [ valgrind ]
          );

        developmentEnvVars =
          with pkgs;
          {
            "WASM_BINDGEN_TEST_TIMEOUT" = "180";

            "CHROME_PATH" = "${chromePath}";
            "CHROME" = "${chromePath}";
            "CHROMEDRIVER" = "${chromedriver}/bin/chromedriver";

            # The Chrome sandbox cannot initialize under the CI runner, so
            # wbg-pool's headless Chrome exits before opening a tab. Test code
            # is trusted, so disable the sandbox for the pooled browser.
            "WBG_POOL_NO_SANDBOX" = "1";
          }
          # A `cargo build --target wasm32` run in this shell compiles the
          # same C as the wasm derivations do, so it needs the same
          # compiler. See `wasmCcEnv` in nix/rust.nix for why the stdenv
          # one will not do.
          // wasmCcEnv
          // lib.optionalAttrs stdenv.isDarwin {
            "WASM_BINDGEN_TEST_WEBDRIVER_JSON" = webdriverConfig;
          };

        # Import menu helpers (e.g., colorful shell commands)
        menuHelpers = (
          import ./nix/menu.nix {
            inherit pkgs;
            # The cross-target tests build their wasm side with cargo when
            # they run, and build it as the archives are built.
            testEnv = testProfile;
          }
        );

        inherit (menuHelpers) makeMenu makeDevShellHook menuTestCommand;

        # What each test configuration builds, as arguments to
        # `cargo nextest archive` (the `tests-*` packages, which the
        # `test:*` commands build and run).
        #
        # wbg-pool is a native host binary and dialog-baseline is a
        # native-only dev measurement crate (its examples pull native-only
        # symbols); neither builds for wasm32, so the web configurations
        # leave them out.
        #
        # Without a target selection cargo also builds every example, 14
        # binaries that no test runs. The debug configurations keep doing
        # that, so an example that stops building for a target still
        # fails here. The others pass `--tests`: an example that builds in
        # debug builds in release.
        testArguments =
          let
            web = "--workspace --exclude wbg-pool --exclude dialog-baseline --target wasm32-unknown-unknown";
          in
          {
            native-debug = "--features integration-tests";
            native-release = "--release --features integration-tests --tests";
            web-debug = web;
            web-release = "${web} --release --tests";
            cross-integration = "${crossTargetPackages} --features web-integration-tests --tests";
          };

        # The packages with cross-target tests: those that declare the
        # `web-integration-tests` feature, read from the manifests so a
        # crate that gains the feature is picked up. The cross-target
        # configuration builds only these. Built workspace-wide it also
        # compiled and ran the tests of every crate without the feature
        # (dialog-query's 970 among them), which are native tests the
        # native configurations already run.
        crossTargetPackages =
          let
            manifest = path: builtins.fromTOML (builtins.readFile (./. + "/${path}"));
            members = map (member: manifest "${member}/Cargo.toml") (manifest "Cargo.toml").workspace.members;
            crossTarget = builtins.filter (member: (member.features or { }) ? "web-integration-tests") members;
          in
          pkgs.lib.concatMapStringsSep " " (member: "--package ${member.package.name}") crossTarget;

        commands = {
          "bench" = {
            description = "Run all benchmarks";
            command = "cargo bench";
          };

          "lint" = {
            description = "Lint the full source tree";
            command = "nix flake check";
          };

          "soak:adversarial" = {
            description = "Adversarial manifest soak (DIALOG_ADVERSARIAL_SEEDS/_OPS override)";
            command = ''
              export DIALOG_ADVERSARIAL_SEEDS="''${DIALOG_ADVERSARIAL_SEEDS:-200}"
              export DIALOG_ADVERSARIAL_OPS="''${DIALOG_ADVERSARIAL_OPS:-1200}"
              cargo test -p dialog-search-tree --release --test adversarial
            '';
          };

          "soak:all" = {
            description = "Run every nightly soak arm back to back";
            command = ''
              soak:adversarial
              soak:program
              soak:facts
              soak:converge
              soak:subscription
              soak:sync
            '';
          };

          "soak:facts" = {
            description = "Facts-level fuzz soak (DIALOG_FACT_FUZZ_SEEDS/_OPS override)";
            command = ''
              export DIALOG_FACT_FUZZ_SEEDS="''${DIALOG_FACT_FUZZ_SEEDS:-200}"
              export DIALOG_FACT_FUZZ_OPS="''${DIALOG_FACT_FUZZ_OPS:-600}"
              cargo test -p dialog-baseline --release --test fact_fuzz
            '';
          };

          "soak:converge" = {
            description = "Deep SE-log convergence replay (DIALOG_CONVERGE_TXNS override)";
            command = ''
              export DIALOG_CONVERGE_TXNS="''${DIALOG_CONVERGE_TXNS:-5000}"
              cargo test -p dialog-baseline --release --test convergence
            '';
          };

          "soak:program" = {
            description = "Program-surface convergence soak (DIALOG_PROGRAM_OPS override)";
            command = ''
              export DIALOG_PROGRAM_OPS="''${DIALOG_PROGRAM_OPS:-2000}"
              cargo test -p dialog-search-tree --release --test program
            '';
          };

          # The randomized subscription invariant from the normal suite,
          # over far more seeds. DIALOG_SUBSCRIPTION_SEEDS is a comma list
          # of seeds and half-open ranges; a failure names the seed and
          # step, and DIALOG_SUBSCRIPTION_SEEDS=<seed> reruns it alone.
          "soak:subscription" = {
            description = "Subscription invariant sweep (DIALOG_SUBSCRIPTION_SEEDS/_STEPS override)";
            command = ''
              export DIALOG_SUBSCRIPTION_SEEDS="''${DIALOG_SUBSCRIPTION_SEEDS:-0..300}"
              export DIALOG_SUBSCRIPTION_STEPS="''${DIALOG_SUBSCRIPTION_STEPS:-100}"
              cargo test -p dialog-repository --release --lib -- --exact \
                repository::branch::subscription::invariants::a_maintained_subscription_matches_a_fresh_evaluation_after_random_changes
            '';
          };

          "perf:gate" = {
            description = "Engine cost matrix (rust/dialog-perf) under callgrind, gated against perf/baseline";
            command = ''
              perf:sweep
              perf:check
            '';
          };

          "perf:sweep" = {
            description = "Sweep the engine cost matrix under callgrind into target/perf-current";
            command = ''
              cargo run -p dialog-perf --release -- sweep --out-dir target/perf-current
            '';
          };

          # Gates target/perf-current against perf/baseline and, when
          # target/perf-base holds another commit's sweep (CI fetches the
          # one a pull request lands on, scripts/ci/base-sweep.sh), against
          # that too, writing every row to target/perf-delta/table.md.
          # Fails if either comparison regresses. Rows that beat their
          # baseline file past the allowance go to target/perf-delta/stale.md
          # (and a warning annotation in CI): flagged, never failed.
          "perf:check" = {
            description = "Gate target/perf-current against perf/baseline and, if present, target/perf-base";
            command = ''
              status=0
              mkdir -p target/perf-delta
              cargo run -p dialog-perf --release -- compare perf/baseline target/perf-current \
                --stale target/perf-delta/stale.md || status=1
              if [ -d target/perf-base ]; then
                rebaselined=$(paste -sd, target/perf-delta/rebaselined 2>/dev/null || true)
                cargo run -p dialog-perf --release -- compare --base target/perf-base target/perf-current \
                  --markdown target/perf-delta/table.md \
                  ''${rebaselined:+--rebaselined "$rebaselined"} || status=1
              fi
              exit $status
            '';
          };

          "soak:gate" = {
            description = "Join cost on the unshaped none profile (rust/dialog-soak), gated against soak/baseline";
            # One fixed seed (DIALOG_SOAK_SEED, unset is the default), so
            # every run writes the same tree and the counts are exact.
            command = ''
              soak:sweep
              soak:check
            '';
          };

          "soak:sweep" = {
            description = "Sweep the join on the unshaped none profile into target/soak-current";
            command = ''
              cargo run -p dialog-soak --release -- sweep --out-dir target/soak-current --networks none
            '';
          };

          # The soak counterpart of perf:check: target/soak-current against
          # soak/baseline and, when present, target/soak-base, with every
          # row in target/soak-delta/table.md. Both runs use the fixed seed,
          # so the base comparison gates as the baseline one does; a base
          # swept under another seed is shown, not gated.
          "soak:check" = {
            description = "Gate target/soak-current against soak/baseline and, if present, target/soak-base";
            command = ''
              status=0
              cargo run -p dialog-soak --release -- compare soak/baseline target/soak-current || status=1
              if [ -d target/soak-base ]; then
                mkdir -p target/soak-delta
                rebaselined=$(paste -sd, target/soak-delta/rebaselined 2>/dev/null || true)
                cargo run -p dialog-soak --release -- compare --base target/soak-base target/soak-current \
                  --markdown target/soak-delta/table.md \
                  ''${rebaselined:+--rebaselined "$rebaselined"} || status=1
              fi
              exit $status
            '';
          };

          "soak:sync" = {
            description = "Sync/join cost sweep over simulated networks, gated against soak/baseline";
            # A fresh seed per run, so the repeats exercise the spread that
            # identities cause; each report records its seed to replay it.
            command = ''
              export DIALOG_SOAK_SEED="''${DIALOG_SOAK_SEED:-random}"
              cargo run -p dialog-soak --release -- sweep --out-dir target/soak-current
              cargo run -p dialog-soak --release -- compare soak/baseline target/soak-current
            '';
          };

          "test:all" = {
            description = "Run the full test suite (all configurations, grab a coffee)";
            command = ''
              test:native:debug
              test:native:release
              test:web:debug
              test:web:release
              test:cross:integration
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
            package = "tests-web-release";
          };

          "test:cross:integration" = menuTestCommand {
            description = "Cross-target integration tests (${system} + wasm32-unknown-unknown, debug)";
            package = "tests-cross-integration";
          };

        };

        menu = makeMenu commands;
      in
      {
        test = commonBuildInputs;

        packages = {
          tests-native-debug = buildTestArchive {
            name = "native-debug";
            args = testArguments.native-debug;
          };

          tests-native-release = buildTestArchive {
            name = "native-release";
            args = testArguments.native-release;
          };

          tests-web-debug = buildTestArchive {
            name = "web-debug";
            args = testArguments.web-debug;
            target = "wasm32-unknown-unknown";
          };

          tests-web-release = buildTestArchive {
            name = "web-release";
            args = testArguments.web-release;
            target = "wasm32-unknown-unknown";
          };

          tests-cross-integration = buildTestArchive {
            name = "cross-integration";
            args = testArguments.cross-integration;
          };
        };

        checks = cargoChecks // {
          # Other checks here...
        };

        devShells = with pkgs; {
          default = mkShell {
            name = "dialog";
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

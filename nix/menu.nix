# This module contains helpers for assembling and printing
# the Dialog Shell menu.
{ pkgs }:
let
  menuGraphic = ''
                                    
                               ++--.         
                               +-.           
               ##+###+##+    ++-.            
           ####+++++##++++++#++.             
         ##+#++++++-+++++++-+-++++           
        ++#+#----+-+-.--------++.+++#        
        +--------+-...---.----....-+++++     
        +-............-  .. .. .....--+++-   
        -.------...  ....................++  
        ##########+-...  ....  .     ..----+ 
       ############+-.....    ....... ....-- 
      ############++--........   ...........-
     ############++--............. ........--
     ###########++---......................--
    #########+++---.......................-- 
    ######+++++-----................---...-  
    ##++++++++------.............----+#      
    #++++++++-------............-----+#      
    #++++++---------.........-------+#       
    #++++---------------...-------++#        
    #+-----------.-.......-.------+#         
     ++-.....-..-..............--+#          
      +--......................--            
       --.....................-              
         -...................                
          ............                       
  '';

  makeMenu =
    commands:
    let
      names = builtins.attrNames commands;

      makeCommand =
        {
          name,
          script,
          description ? "<No description given>",
          env ? { },
        }:
        {
          inherit name description;

          package =
            with pkgs;
            writeShellApplication {
              inherit name;
              runtimeEnv = env;
              text = ''
                TITLE="$(${figlet}/bin/figlet -t '${name}')"
                SUBTITLE="${description}"

                echo "$TITLE
                $SUBTITLE
                " | ${lolcat}/bin/lolcat

                ${script}
              '';
            };
        };

      intoPackages =
        name:
        let
          element = builtins.getAttr name commands;

          task = makeCommand {
            inherit name;
            description = element.description;
            script = element.command;
            env = if builtins.hasAttr "env" element then element.env else { };
          };
        in
        task.package;

      intoLines =
        acc: name:
        let
          description = (builtins.getAttr name commands).description;
        in
        acc + " && echo '${name};${description}'";

      scripts = map intoPackages names;

      menuLines = builtins.foldl' intoLines "echo ''" names;

      menu = ''
        echo "$(${menuLines})" | column -t -s ';'
      '';
    in
    {
      header = ''
        echo "${menuGraphic}

        $(${pkgs.figlet}/bin/figlet -t "Dialog DB")

        $(${menu})
        " | ${pkgs.lolcat}/bin/lolcat;
      '';
      menuText = ''
        echo "$(${menu})" | ${pkgs.lolcat}/bin/lolcat
      '';
      commands = scripts;
    };

  makeDevShellHook =
    { header, menuText, ... }:
    ''
      clear
      ${header}

      function showMenu() {
        ${menuText}
      }

      export -f showMenu
    '';

  # Ensures CHROME and CHROMEDRIVER are set before running browser tests.
  # On Linux these are provided by menuTestEnv via Nix packages, so this
  # is a no-op. On macOS, chromium/chromedriver are not available in nixpkgs
  # (https://github.com/NixOS/nixpkgs/issues/247855), so we detect them
  # from the system and guide the user to install them if missing.
  ensureBrowser = pkgs.writeShellApplication {
    name = "ensure-browser";
    text = ''
      if [ -z "''${CHROME:-}" ]; then
        CHROME_DEFAULT="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        if [ -x "$CHROME_DEFAULT" ]; then
          printf 'export CHROME=%q\n' "$CHROME_DEFAULT"
        else
          echo "Error: Chrome not found. Install Google Chrome or set the CHROME env var." >&2
          exit 1
        fi
      else
        printf 'export CHROME=%q\n' "$CHROME"
      fi

      if [ -z "''${CHROMEDRIVER:-}" ]; then
        CHROMEDRIVER_PATH="$(command -v chromedriver 2>/dev/null || true)"
        if [ -n "$CHROMEDRIVER_PATH" ]; then
          printf 'export CHROMEDRIVER=%q\n' "$CHROMEDRIVER_PATH"
        else
          echo "Error: chromedriver not found in PATH." >&2
          echo "Install it with: brew install --cask chromedriver" >&2
          exit 1
        fi
      else
        printf 'export CHROMEDRIVER=%q\n' "$CHROMEDRIVER"
      fi
    '';
  };

  makeMenuTestCommand = package: ''
    eval "$(${ensureBrowser}/bin/ensure-browser)"

    nix build .#${package}

    TESTS_PATH=$(nix eval .#${package}.outPath --raw)

    cargo nextest run \
      --workspace-remap ./ \
      --archive-file "$TESTS_PATH/${package}.tar.zst" \
  '';

  # Runs a single wasm test binary through wasm-bindgen-test-runner,
  # buffering its output so parallel invocations don't interleave. Prints
  # the one-line libtest summary on success and the full log on failure.
  runWasmSuite = pkgs.writeShellApplication {
    name = "run-wasm-suite";
    text = ''
      binary="$1"
      name="$(basename "$binary" .wasm)"
      log="$(mktemp)"
      if wasm-bindgen-test-runner "$binary" > "$log" 2>&1; then
        sed -n "s/^test result:/[$name]/p" "$log"
        rm -f "$log"
      else
        echo "[$name] FAILED:"
        cat "$log"
        rm -f "$log"
        exit 1
      fi
    '';
  };

  # wasm-bindgen-test-runner starts a fresh chromedriver + Chrome and
  # compiles the wasm module on every invocation, which costs ~6 seconds
  # before any test runs. `cargo nextest run` invokes the runner once per
  # test, so on a ~1400-test suite that startup tax dominates the run
  # (~85 minutes in CI). Instead, extract the archive, list the test
  # binaries, and invoke the runner once per binary: startup is paid ~30
  # times rather than ~1400, which makes the same suite run in minutes.
  makeWebMenuTestCommand = package: ''
    eval "$(${ensureBrowser}/bin/ensure-browser)"

    nix build .#${package}

    TESTS_PATH=$(nix eval .#${package}.outPath --raw)

    EXTRACT_DIR="$(mktemp -d)"
    trap 'rm -rf "$EXTRACT_DIR"' EXIT

    cargo nextest list \
      --workspace-remap ./ \
      --archive-file "$TESTS_PATH/${package}.tar.zst" \
      --extract-to "$EXTRACT_DIR" \
      --message-format json > "$EXTRACT_DIR/suites.json"

    ${pkgs.jq}/bin/jq -r '
      ."rust-suites"[]
      | select((.testcases | length) > 0)
      | ."binary-path"
    ' "$EXTRACT_DIR/suites.json" > "$EXTRACT_DIR/binaries.txt"

    echo "Running $(wc -l < "$EXTRACT_DIR/binaries.txt") wasm test binaries"

    if [ -s "$EXTRACT_DIR/binaries.txt" ]; then
      xargs -P "$(${pkgs.coreutils}/bin/nproc)" -n 1 \
        ${runWasmSuite}/bin/run-wasm-suite < "$EXTRACT_DIR/binaries.txt"
    fi
  '';

  menuTestEnv =
    with pkgs;
    lib.optionalAttrs stdenv.isLinux {
      "CHROME" = "${chromium}/bin/chromium";
      "CHROMEDRIVER" = "${chromedriver}/bin/chromedriver";
    };

  menuTestCommand =
    { description, package }:
    {
      inherit description;
      command = makeMenuTestCommand package;
      env = menuTestEnv;
    };

  menuWebTestCommand =
    { description, package }:
    {
      inherit description;
      command = makeWebMenuTestCommand package;
      env = menuTestEnv;
    };
in
{
  inherit
    makeMenu
    makeDevShellHook
    menuTestCommand
    menuWebTestCommand
    ;
}

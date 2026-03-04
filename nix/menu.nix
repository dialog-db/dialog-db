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
in
{
  inherit makeMenu makeDevShellHook menuTestCommand;
}

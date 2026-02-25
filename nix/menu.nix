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

  makeMenuTestCommand = package: ''
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

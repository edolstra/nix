with import ./config.nix;

rec {

    a = mkDerivation {
        name = "group";
        ownershipGroup = "nogroup";
        builder = builtins.toFile "builder.sh" ''
            # Apply various modes in the builder to be cleared by Nix.
            mkdir -p $out/foo
            chmod 1777 $out/foo
            touch $out/foo/secure
            chmod 0400 $out/foo/secure
            touch $out/foo/setuid
            chmod +x $out/foo/setuid
            chmod u+s $out/foo/setuid || true
            chmod g+s $out/foo/setuid || true
        '';
    };

}

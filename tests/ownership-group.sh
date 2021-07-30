source common.sh

echo "building test path to test group ownership"
storePath="$(nix-build ownership-group.nix -A a --no-out-link)"
find $storePath -ls

# Verify all mode of all files.
test "$(stat --format %A $storePath/foo)" = "dr-xr-x---"
test "$(stat --format %A $storePath/foo/secure)" = "-r--r-----"
test "$(stat --format %A $storePath/foo/setuid)" = "-r-xr-x---"

# Unfortunately cannot verify owner/group because tests don't run as root.

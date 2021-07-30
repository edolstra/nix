source common.sh

echo "building test path to test user ownership"
storePath="$(nix-build ownership-user.nix -A a --no-out-link)"
find $storePath -ls

# Verify all mode of all files.
test "$(stat --format %A $storePath/foo)" = "dr-x------"
test "$(stat --format %A $storePath/foo/secure)" = "-r--------"
test "$(stat --format %A $storePath/foo/setuid)" = "-r-x------"

# Unfortunately cannot verify owner/group because tests don't run as root.

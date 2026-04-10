#!/bin/sh -e

# Apply hotfix patches to lib submodules.
# Patches are stored in lib/patches/<submodule>/*.patch and applied
# in sorted order after `git submodule update --init`.
# This script is called from autogen.sh.

case "$0" in
    */*)
        cd "$(dirname "$0")/.."
        ;;
esac

PATCHES_DIR="lib/patches"

if [ ! -d "$PATCHES_DIR" ]; then
    exit 0
fi

applied=0

for submod_dir in "$PATCHES_DIR"/*/; do
    submod=$(basename "$submod_dir")
    target="lib/$submod"

    if [ ! -d "$target" ]; then
        continue
    fi

    patches=$(find "$submod_dir" -maxdepth 1 -name '*.patch' 2>/dev/null | sort)
    if [ -z "$patches" ]; then
        continue
    fi

    for patch in $patches; do
        echo "Applying patch: $patch -> $target"
        git apply --directory="$target" "$patch"
        applied=$((applied + 1))
    done
done

if [ "$applied" -gt 0 ]; then
    echo "Applied $applied patch(es) to lib submodules."
else
    echo "No lib submodule patches to apply."
fi

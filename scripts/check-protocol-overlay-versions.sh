#!/bin/sh

# This script checks that a change that bumps the current ledger protocol
# version (Config::CURRENT_LEDGER_PROTOCOL_VERSION in src/main/Config.cpp)
# also bumps the maximum supported overlay protocol version
# (OVERLAY_PROTOCOL_VERSION in the same file), so that peers can tell from
# the overlay handshake whether a node runs software that supports the new
# ledger protocol.
#
# Usage: check-protocol-overlay-versions.sh [--release <rev> | --fetch-release <remote>]
#                                          <base-rev> [<head-rev>]
#
# Compares src/main/Config.cpp at <head-rev> (or the working tree copy if
# <head-rev> is omitted) against <base-rev> and against the last release.
# The last release is either given explicitly with --release, or found with
# --fetch-release, which fetches the highest vX.Y.Z tag (release candidates
# excluded) from <remote>.
#
# Versions are compared against the last release rather than only against
# <base-rev>, so that overlay is bumped once per release, not once per ledger
# protocol bump: if master is already a ledger protocol ahead of the last
# release and has bumped the overlay version for it, a further ledger
# protocol bump needs no further overlay bump.
#
# If the change modifies either version, the script exits with a non-zero
# status if, relative to the last release,
#   - CURRENT_LEDGER_PROTOCOL_VERSION increased but OVERLAY_PROTOCOL_VERSION
#     did not, or
#   - OVERLAY_PROTOCOL_VERSION increased by more than one (bumped twice).
# If the change modifies neither version, problems already present at
# <base-rev> are reported as warnings only, so they don't block unrelated
# changes.
#
# If no release can be found, the check falls back to comparing against
# <base-rev> only.

set -e

SRCDIR=$(realpath $(dirname $0)/..)
CONFIG_CPP=src/main/Config.cpp

usage()
{
    echo "usage: $0 [--release <rev> | --fetch-release <remote>] <base-rev> [<head-rev>]" >&2
    exit 2
}

RELEASE_REV=
FETCH_REMOTE=
while [ $# -gt 0 ]
do
    case "$1" in
    --release)
        [ $# -ge 2 ] || usage
        RELEASE_REV=$2
        shift 2
        ;;
    --fetch-release)
        [ $# -ge 2 ] || usage
        FETCH_REMOTE=$2
        shift 2
        ;;
    -*)
        usage
        ;;
    *)
        break
        ;;
    esac
done

if [ $# -lt 1 ] || [ $# -gt 2 ] || { [ -n "$RELEASE_REV" ] && [ -n "$FETCH_REMOTE" ]; }
then
    usage
fi

BASE_REV=$1
HEAD_REV=${2:-}

cd "$SRCDIR"

if [ -n "$FETCH_REMOTE" ]
then
    RELEASE_TAG=$(git ls-remote --tags --refs "$FETCH_REMOTE" 'v*' |
                  sed -n 's|.*refs/tags/\(v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)$|\1|p' |
                  sort -V | tail -n 1)
    if [ -n "$RELEASE_TAG" ] &&
       git fetch --quiet --depth=1 "$FETCH_REMOTE" "refs/tags/$RELEASE_TAG:refs/tags/$RELEASE_TAG"
    then
        RELEASE_REV=$RELEASE_TAG
    else
        echo "warning: could not fetch the last release from $FETCH_REMOTE;" >&2
        echo "comparing against $BASE_REV only." >&2
    fi
fi

# Print the contents of src/main/Config.cpp at revision $1, or the working
# tree copy if $1 is empty.
config_at()
{
    if [ -n "$1" ]
    then
        git show "$1:$CONFIG_CPP"
    else
        cat "$CONFIG_CPP"
    fi
}

# extract <rev> <name> <sed-expr>: print the numeric value assigned to
# <name> in Config.cpp at <rev>, failing unless exactly one assignment is
# found.
extract()
{
    if ! CONTENTS=$(config_at "$1")
    then
        echo "error: failed to read $CONFIG_CPP at ${1:-working tree}" >&2
        exit 2
    fi
    VAL=$(printf '%s\n' "$CONTENTS" | sed -n "$3")
    case "$VAL" in
    ''|*[!0-9]*)
        echo "error: expected exactly one numeric assignment to $2 in $CONFIG_CPP at ${1:-working tree}, got '$VAL'" >&2
        echo "(if the definition of $2 changed shape, update $0 to match)" >&2
        exit 2
        ;;
    esac
    echo "$VAL"
}

LEDGER_EXPR='s/.*Config::CURRENT_LEDGER_PROTOCOL_VERSION[[:space:]]*=[[:space:]]*\([0-9][0-9]*\).*/\1/p'
OVERLAY_EXPR='s/^[[:space:]]*OVERLAY_PROTOCOL_VERSION[[:space:]]*=[[:space:]]*\([0-9][0-9]*\).*/\1/p'

BASE_LEDGER=$(extract "$BASE_REV" CURRENT_LEDGER_PROTOCOL_VERSION "$LEDGER_EXPR")
BASE_OVERLAY=$(extract "$BASE_REV" OVERLAY_PROTOCOL_VERSION "$OVERLAY_EXPR")
HEAD_LEDGER=$(extract "$HEAD_REV" CURRENT_LEDGER_PROTOCOL_VERSION "$LEDGER_EXPR")
HEAD_OVERLAY=$(extract "$HEAD_REV" OVERLAY_PROTOCOL_VERSION "$OVERLAY_EXPR")

if [ -n "$RELEASE_REV" ]
then
    REF_NAME="last release ($RELEASE_REV)"
    REF_LEDGER=$(extract "$RELEASE_REV" CURRENT_LEDGER_PROTOCOL_VERSION "$LEDGER_EXPR")
    REF_OVERLAY=$(extract "$RELEASE_REV" OVERLAY_PROTOCOL_VERSION "$OVERLAY_EXPR")
    echo "CURRENT_LEDGER_PROTOCOL_VERSION: release $REF_LEDGER, base $BASE_LEDGER, head $HEAD_LEDGER"
    echo "OVERLAY_PROTOCOL_VERSION:        release $REF_OVERLAY, base $BASE_OVERLAY, head $HEAD_OVERLAY"
else
    REF_NAME="base ($BASE_REV)"
    REF_LEDGER=$BASE_LEDGER
    REF_OVERLAY=$BASE_OVERLAY
    echo "CURRENT_LEDGER_PROTOCOL_VERSION: base $BASE_LEDGER, head $HEAD_LEDGER"
    echo "OVERLAY_PROTOCOL_VERSION:        base $BASE_OVERLAY, head $HEAD_OVERLAY"
fi

# Only fail if this change touches either version; otherwise only warn
# about problems inherited from the base.
if [ "$HEAD_LEDGER" -ne "$BASE_LEDGER" ] || [ "$HEAD_OVERLAY" -ne "$BASE_OVERLAY" ]
then
    LEVEL=error
else
    LEVEL=warning
fi

FAILED=
if [ "$HEAD_LEDGER" -gt "$REF_LEDGER" ] && [ "$HEAD_OVERLAY" -le "$REF_OVERLAY" ]
then
    echo "$LEVEL: CURRENT_LEDGER_PROTOCOL_VERSION was bumped from $REF_LEDGER to $HEAD_LEDGER" >&2
    echo "since the $REF_NAME without bumping OVERLAY_PROTOCOL_VERSION (still $HEAD_OVERLAY)." >&2
    echo "A ledger protocol bump must be accompanied by a bump of the maximum" >&2
    echo "overlay protocol version in $CONFIG_CPP." >&2
    FAILED=1
fi

if [ "$HEAD_OVERLAY" -gt $((REF_OVERLAY + 1)) ]
then
    echo "$LEVEL: OVERLAY_PROTOCOL_VERSION was bumped from $REF_OVERLAY to $HEAD_OVERLAY" >&2
    echo "since the $REF_NAME. It only needs to be bumped once per release." >&2
    FAILED=1
fi

if [ -n "$FAILED" ]
then
    if [ "$LEVEL" = error ]
    then
        exit 1
    fi
    echo "OK (with warnings: this change doesn't modify either version)"
else
    echo "OK"
fi

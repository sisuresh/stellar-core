# Lib Submodule Patches

This directory holds hotfix patches for third-party C++ submodules in `lib/`.
Patches are applied automatically by `lib/apply-patches.sh` during `autogen.sh`,
after `git submodule update --init` and before the build.

## Supported submodules

- `libsodium/` — stellar/libsodium
- `xdrpp/` — xdrpp/xdrpp
- `libmedida/` — stellar/medida
- `cereal/` — USCiLab/cereal
- `asio/` — chriskohlhoff/asio
- `fmt/` — fmtlib/fmt
- `tracy/` — stellar/tracy
- `spdlog/` — gabime/spdlog
- `gperftools/` — gperftools/gperftools

## Creating a patch

```bash
cd lib/<submodule>
# make your fix
git diff > ../../lib/patches/<submodule>/001-description.patch
# restore the submodule to its pinned state
git checkout -- .
```

Patches are applied in sorted order, so use a numeric prefix (001-, 002-, …).

## Applying patches

Patches are applied automatically when you run `./autogen.sh`. You can also
apply them manually:

```bash
./lib/apply-patches.sh
```

The script fails the build if a patch does not apply cleanly, which signals that
the submodule has been updated and the patch needs rebasing.

## Removing a hotfix

Delete the `.patch` file and re-run `./autogen.sh` (which resets submodules
to their pinned commits before re-applying remaining patches).

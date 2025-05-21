#!/usr/bin/env bash

set -eufo pipefail

BUILD_TARGET=dialog-remote-cloudflare-worker
BUILD_FOLDER=$BUILD_TARGET

nix build .#$BUILD_TARGET
cp -r ./result/$BUILD_FOLDER $TMPDIR
cd $TMPDIR
chmod -R 755 ./$BUILD_FOLDER
cd ./$BUILD_FOLDER
wrangler deploy

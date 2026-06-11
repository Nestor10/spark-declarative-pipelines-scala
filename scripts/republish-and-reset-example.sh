#!/usr/bin/env bash
# Republish both modules and reset the sdp-example consumer so it can't
# resolve a stale SNAPSHOT. Needed after a CODEC/FORMAT change (manifest, RelCodec)
# — the fragment/manifest codec lives in the sdp library consumed by BOTH the
# user build and the plugin metabuild, and the consumer's metabuild caches the old
# plugin closure. (Plain logic changes usually don't need this.)
#
# Usage: scripts/republish-and-reset-example.sh   (run from the main repo root)
set -euo pipefail
EXAMPLE="${1:-../sdp-example}"

echo "▸ publishing sdp + sbt-spark-pipelines (Local + M2)…"
sbt --client "sdp/publishLocal" >/dev/null && sbt --client "sdp/publishM2" >/dev/null && sbt --client "sbtSparkPipelines/publishLocal" >/dev/null && sbt --client "sbtSparkPipelines/publishM2" >/dev/null

echo "▸ evicting io.github.nestor10 from ivy + coursier caches…"
# Maven groupId is io.github.nestor10 (Scala packages stay dev.sdp). Also clean
# any leftover dev.sdp artifacts from before the groupId change — harmless if absent.
rm -rf ~/.ivy2/cache/io.github.nestor10 ~/.ivy2/cache/dev.sdp 2>/dev/null || true
find ~/Library/Caches/Coursier \( -path "*io/github/nestor10*" -o -path "*dev/sdp*" \) -prune -exec rm -rf {} + 2>/dev/null || true

if [ -d "$EXAMPLE" ]; then
  echo "▸ clearing $EXAMPLE metabuild + build targets…"
  rm -rf "$EXAMPLE/project/target" "$EXAMPLE/project/project" "$EXAMPLE/target" "$EXAMPLE/.bsp" 2>/dev/null || true
fi

echo "✓ done. Restart any running sbt session in $EXAMPLE (exit + sbt) to pick up the fresh plugin."

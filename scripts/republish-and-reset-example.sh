#!/usr/bin/env bash
# Republish all three modules and reset the sdp-example consumer so it can't
# resolve a stale SNAPSHOT. Needed after a CODEC/FORMAT change (manifest, RelCodec)
# — those require the macro embedder (sdp-runtime-dsl) and the plugin's parser
# (sdp-core) to move in lockstep, and the consumer's metabuild caches the old
# plugin closure. (Plain logic changes usually don't need this.)
#
# Usage: scripts/republish-and-reset-example.sh   (run from the main repo root)
set -euo pipefail
EXAMPLE="${1:-../sdp-example}"

echo "▸ publishing sdp-core, sdp-runtime-dsl, sbt-spark-pipelines (Local + M2)…"
sbt --client "sdpCore/publishLocal; sdpCore/publishM2; sdpRuntimeDsl/publishLocal; sdpRuntimeDsl/publishM2; sbtSparkPipelines/publishLocal; sbtSparkPipelines/publishM2" >/dev/null

echo "▸ evicting dev.sdp from ivy + coursier caches…"
rm -rf ~/.ivy2/cache/dev.sdp 2>/dev/null || true
find ~/Library/Caches/Coursier -path "*dev/sdp*" -prune -exec rm -rf {} + 2>/dev/null || true

if [ -d "$EXAMPLE" ]; then
  echo "▸ clearing $EXAMPLE metabuild + build targets…"
  rm -rf "$EXAMPLE/project/target" "$EXAMPLE/project/project" "$EXAMPLE/target" "$EXAMPLE/.bsp" 2>/dev/null || true
fi

echo "✓ done. Restart any running sbt session in $EXAMPLE (exit + sbt) to pick up the fresh plugin."

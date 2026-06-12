# Releasing

Two artifacts ship together (D11): **`sdp`** (the library) and
**`sbt-spark-pipelines`** (the sbt plugin), both under groupId
`io.github.nestor10`. Publishing is **tag-driven via GitHub Actions** —
the same `sbt ci-release` flow as fishy-mcp (whose Central publishes already
established the `io.github.nestor10` namespace and PGP key), verified working
on sbt 2.0.0-RC16 (`sbt-ci-release_sbt2_3:1.11.2`).

## One-time setup (copy from fishy-mcp)

The release workflow needs the same four repository secrets fishy-mcp uses
(Settings -> Secrets and variables -> Actions):

- `SONATYPE_USERNAME` / `SONATYPE_PASSWORD` — the Central Portal user token.
- `PGP_SECRET` / `PGP_PASSPHRASE` — the base64-exported signing key.

Nothing else: the namespace is already verified, the POM metadata
(licenses/scm/developers) lives in build.sbt, and sources/javadoc jars are
produced by default.

## Cutting a release

1. **Pre-release checklist**
   - [ ] `ROADMAP.md` reflects reality; completed items moved out.
   - [ ] **CoreEpoch (D6, softened by D11):** if this release changes the
         fragment/manifest codec (`RelCodec`, `LineCodec`, `PipelineManifest`,
         `PipelineExport`), bump `dev.sdp.core.CoreEpoch.value`. Since the
         D11 collapse the old cross-artifact inlining hazard is mostly gone
         (core/dsl/connect are one jar); the epoch now guards the remaining
         sdp <-> plugin boundary and consumer action caches. Cheap insurance —
         when in doubt, bump.
   - [ ] Green locally: `sbt Test/compile`, `sbt sdp/testFull`,
         `sbt sbtSparkPipelines/scripted`.
   - [ ] Smoke the consumer: `scripts/republish-and-reset-example.sh`, then
         `sdpValidate` + `sdpDryRun` in `../sdp-example`.
2. **Tag and push** — version comes from the tag (sbt-dynver); never set
   `ThisBuild / version` by hand:

   ```
   git tag -a v0.1.0 -m "v0.1.0"
   git push origin v0.1.0
   ```

3. **Watch the Release workflow** (`.github/workflows/release.yml`): it runs
   `sbt ci-release`, which signs both artifacts and uploads the bundle to the
   Central Portal. Artifacts appear on Maven Central within ~30 minutes.
4. **Post-release**
   - [ ] Point `../sdp-example`'s `plugins.sbt` at the released version and
         drop its `Resolver.mavenLocal` line — the honest-consumer test.
   - [ ] Update `ROADMAP.md` / `DECISIONS.md` as warranted.

## Between releases

Untagged builds get dynver snapshot versions (`X.Y.Z+N-<hash>-SNAPSHOT`).
Local consumer testing still uses `scripts/republish-and-reset-example.sh`
(publishes `sdp` + the plugin to both `~/.ivy2/local` and `~/.m2`, evicts
caches, resets the example metabuild — needed because the example resolves
like a real consumer via `Resolver.mavenLocal`).

## Verified vs untested

- **Verified:** sbt-ci-release 1.11.2 resolves and loads on sbt 2.0.0-RC16;
  dynver derives versions from git; publishLocal/publishM2 of both artifacts;
  POM completeness (licenses/scm/developers/url blocks present); scripted
  suite green under the release setup.
- **Untested until the first tag:** the full `ci-release` signing + Portal
  upload from CI for THIS repo (the identical flow is proven from fishy-mcp,
  but on sbt 1 — first tag here validates it on sbt 2), and the plugin's
  `_sbt2_3` artifact passing Central Portal validation.

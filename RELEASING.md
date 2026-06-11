# Releasing `sbt-spark-pipelines`

The runbook for cutting an immutable, tagged release of the four published
modules to **Maven Central** via the **Sonatype Central Portal**.

- **Maven groupId:** `io.github.nestor10` (Central's GitHub-verified namespace
  for `github.com/Nestor10`). The Scala *package* names stay `dev.sdp.*` — only
  the Maven coordinates use the namespace.
- **Published artifacts (Scala 3 / sbt 2):**
  - `io.github.nestor10 %% sdp-core % <v>`        (`sdp-core_3`)
  - `io.github.nestor10 %% sdp-runtime-dsl % <v>` (`sdp-runtime-dsl_3`)
  - `io.github.nestor10 %% sdp-connect % <v>`     (`sdp-connect_3`)
  - `io.github.nestor10 % sbt-spark-pipelines % <v>` (Maven layout, suffix
    `_sbt2_3` — `sbtPluginPublishLegacyMavenStyle := false`)
- The root aggregate is `publish / skip := true` and is never published.

> **Honesty note.** The `publishLocal` / `publishM2` smoke steps and the POM
> contents are verifiable in-repo today. The actual *Central upload* steps below
> are **untested from this repo** (no Sonatype account / GPG key wired yet) and
> describe the documented Central Portal flow. Treat the upload section as a
> checklist to validate on the first real release, not a proven script.

---

## Prerequisites (one-time, done by the maintainer — out of scope of the repo)

1. **Sonatype Central account** with the `io.github.nestor10` namespace verified
   (Central verifies GitHub namespaces by a one-time challenge repo/commit).
2. **A published GPG key.** Generate (`gpg --gen-key`), then publish the public
   key to a keyserver Central checks (e.g. `keys.openpgp.org`):
   ```
   gpg --keyserver keys.openpgp.org --send-keys <KEY_ID>
   ```
3. **A Central Portal user token** (Account → Generate User Token). It yields a
   username/password pair used either for the Portal API or as Sonatype
   credentials.

None of the above is stored in the repo. Keep keys and tokens out of git.

---

## Pre-release checklist

- [ ] **`CoreEpoch` bump (CRITICAL — DECISIONS.md D6).**
      `sdp-core/src/main/scala/dev/sdp/core/CoreEpoch.scala` holds
      `final val value`. It is a compile-time constant **inlined** into
      `sdp-runtime-dsl`'s classfiles/TASTy. sbt 2's content-addressed cache
      will replay downstream compiles with stale macro-embedded constants if a
      pure-`sdp-core` change leaves the dsl bytecode bit-identical — surviving
      `clean`. **Rule: bump `CoreEpoch.value` on every `sdp-core` release** (and
      update its trailing comment with the date + reason). Skipping this can ship
      a consumer that resolves a new jar but keeps old macro-baked behavior.
- [ ] `sbt testFull` green across all four modules.
- [ ] `sbt sbtSparkPipelines/scripted` green (publishes locally first).
- [ ] `golden-renders.txt` unchanged (no unintended algebra/render drift).
- [ ] `ROADMAP.md` reflects what's actually shipping; `DECISIONS.md` updated if a
      decision changed.
- [ ] Decide the version number (early-semver; `versionScheme := "early-semver"`
      is set). First tag: **`0.1.0`**.

---

## 1. Set the release version

`ThisBuild / version` defaults to `0.1.0-SNAPSHOT` (dev). For a release we set
the version explicitly on the command line — there is **no sbt-dynver/ci-release
plugin** wired (those have no verified `_sbt2_3` build as of sbt 2.0.0-RC15, and
we don't add unverifiable plugins). So release versioning is **manual**:

```
sbt 'set ThisBuild / version := "0.1.0"' <publish commands>
```

Everything below assumes you prefix the publish invocation with that `set`, or
edit `build.sbt`'s `ThisBuild / version` for the duration of the release and
revert after (step 5).

---

## 2. Local smoke (verified)

Confirm POMs and artifacts before anything leaves your machine:

```
sbt 'set ThisBuild / version := "0.1.0"' \
  'sdpCore/publishLocal; sdpRuntimeDsl/publishLocal; sdpConnect/publishLocal; sbtSparkPipelines/publishLocal'
```

Then inspect the generated POMs (under `~/.ivy2/local/io.github.nestor10/...`
for `publishLocal`, or `~/.m2/repository/io/github/nestor10/...` for
`publishM2`). Each library POM must contain `<licenses>` (Apache-2.0),
`<scm>`, `<developers>`, and `<url>` (homepage). These come from the
`ThisBuild` POM settings in `build.sbt`.

`publishM2` writes the same Maven layout Central expects — use it to stage a
manual bundle:

```
sbt 'set ThisBuild / version := "0.1.0"' \
  'sdpCore/publishM2; sdpRuntimeDsl/publishM2; sdpConnect/publishM2; sbtSparkPipelines/publishM2'
```

Central requires **sources** and **javadoc** jars alongside each artifact; sbt
produces both by default (we never disabled `packageSrc`/`packageDoc`). Verify
`*-sources.jar` and `*-javadoc.jar` appear in `~/.m2/repository/...`.

---

## 3. Publish to Central (untested — validate on first release)

Two routes. **Route B (manual Portal bundle) is the recommended path** because
it needs no unverified sbt plugin.

### Route A — sbt-driven sign + publish (optional, unverified for sbt 2)

`sbt-pgp` 2.3.0 advertises sbt 2.x cross-builds and `sbt-sonatype` may follow,
but neither is wired into this build and neither is confirmed to ship a working
`_sbt2_3` artifact here. If you choose to try it:

1. Add to `project/plugins.sbt` (the metabuild runs on Scala 3 — the plugin
   MUST publish a Scala 3 / sbt 2 artifact, or this step is a non-starter):
   ```scala
   addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.0")
   // a sonatype/central-portal publishing plugin IF one exists for sbt 2
   ```
2. `sbt 'set ThisBuild / version := "0.1.0"' publishSigned`
3. Release the staging repo (plugin-specific command).

If `addSbtPlugin` fails to resolve an sbt-2 artifact, abandon Route A and use B.

### Route B — manual signed bundle upload to the Central Portal (recommended)

1. **Stage** all artifacts into a local Maven layout with `publishM2` (step 2).
2. **Sign** every artifact (jars, sources, javadoc, and the `.pom`) with GPG.
   For each file under `~/.m2/repository/io/github/nestor10/...`:
   ```
   gpg --armor --detach-sign <file>
   ```
   producing a `.asc` next to each. (Central also wants `.md5`/`.sha1`
   checksums; `publishM2` already writes these — confirm they're present.)
3. **Zip the bundle** preserving the `io/github/nestor10/...` directory layout:
   ```
   cd ~/.m2/repository
   zip -r /tmp/sdp-0.1.0-bundle.zip io/github/nestor10
   ```
4. **Upload** `sdp-0.1.0-bundle.zip` at <https://central.sonatype.com> →
   *Publish* → *Upload a Deployment Bundle*, authenticating with the Central
   Portal user token (step "Prerequisites").
5. The Portal validates the bundle (POM completeness, signatures, sources +
   javadoc). Fix any reported gaps, then **Publish** to release to Central.
   Propagation to `repo1.maven.org` typically takes minutes to a few hours.

> The `publishTo` setting in `build.sbt` points snapshots at the Central
> snapshot repo and releases at the Portal API endpoint. Route A would use it
> directly; Route B ignores it (the bundle is uploaded by hand).

---

## 4. Tag the release

Only after Central accepts the bundle, create the immutable tag:

```
git tag -a v0.1.0 -m "sbt-spark-pipelines 0.1.0"
git push origin v0.1.0
```

(Tag from the commit you actually published. If you edited `build.sbt`'s
version, tag the commit that bears `0.1.0`; if you used the `set` override, tag
the dev commit and record the version in the tag message.)

---

## 5. Post-release

- [ ] **Bump back to the next SNAPSHOT.** If you edited `build.sbt`, set
      `ThisBuild / version := "0.1.1-SNAPSHOT"` (or next minor) and commit. If
      you used the `set` override, nothing to revert — the repo stays on
      `0.1.0-SNAPSHOT` until you choose the next dev line.
- [ ] **Verify the consumer resolves the release.** In `../sdp-example`, pin
      `addSbtPlugin("io.github.nestor10" % "sbt-spark-pipelines" % "0.1.0")`
      (replacing the SNAPSHOT), clear caches
      (`scripts/republish-and-reset-example.sh` evicts `io.github.nestor10`),
      and confirm `sbt sdpManifest` resolves the released coordinates from
      Central — not from `~/.ivy2/local`. The version-lockstep injection in the
      plugin (`SdpBuildInfo.organization %% "sdp-runtime-dsl" % SdpBuildInfo.version`)
      pulls `io.github.nestor10 %% sdp-runtime-dsl % 0.1.0` and
      `io.github.nestor10 %% sdp-connect % 0.1.0` automatically.
- [ ] **Update `ROADMAP.md`:** move "Release prep" out of remaining work; record
      the released version in the docs.
- [ ] Announce / update `README` install snippets to the released version.

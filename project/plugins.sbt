// Metabuild plugins. Intentionally empty for now — add scalafmt, scalafix, etc.
// here when needed. The sbt 2.0 metabuild runs on Scala 3, so any plugin
// added here must publish a Scala 3 artifact.

// Tag-driven publishing to Maven Central (Sonatype Central Portal) — the same
// flow as fishy-mcp, same version, which IS published for sbt 2 (_sbt2_3
// verified on repo1). Brings sbt-dynver (version from git tag), sbt-pgp
// (signing), sbt-sonatype (bundle upload). Driven by .github/workflows/release.yml.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.2")

// Dependency freshness: `dependencyUpdates` lists newer versions of every
// libraryDependency (0.7.0 is the first release published for sbt 2, _sbt2_3).
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.7.0")


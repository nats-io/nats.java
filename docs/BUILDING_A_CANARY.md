# Building a canary against jnats

A "canary" is a downstream repo that builds and tests itself against jnats' **current snapshot** on a schedule, so a change in jnats that breaks a consumer is caught early - in the consumer's own CI - rather than after a release.

It's entirely on the **consumer's** side. jnats does nothing and needs no changes: the canary just reads jnats' public state. There's no token, no notification, no coordination.

## How it works

Once a day the canary:

1. reads jnats' current dev version from `build.gradle` on jnats main (`def jarVersion = "X"`); the snapshot jnats publishes from main is `X-SNAPSHOT`.
2. rewrites its own `build.gradle` to depend on `X-SNAPSHOT` - **for that CI run only, never committed**.
3. builds and tests with `--refresh-dependencies` so it pulls the newest snapshot content.

Reading jnats' source `jarVersion` (rather than the snapshots repo's `<latest>`) keeps the canary on the real current line and immune to any stray/accidental published version.

## Drop-in workflow

Add this to the consumer repo as `.github/workflows/jnats-canary.yml`. The only part to adapt is the build setup (JDK/Gradle/etc.) - make it match the consumer's own `build-pr.yml`; the jnats-specific bits (read version, rewrite, refresh) stay as-is. Two forms below: a **single build**, and a **matrix** for repos whose `build-pr.yml` tests across several targets.

### Single build

```yaml
name: jnats canary

on:
  schedule:
    - cron: "17 3 * * *"          # once a day at 03:17 UTC - before the European workday, so results are ready by morning
  workflow_dispatch:               # run on demand from the Actions tab ("Run workflow")
    inputs:
      version:
        description: "jnats version to test (blank = jnats' current main snapshot, e.g. 2.27.0 or 2.27.0-SNAPSHOT)"
        required: false

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - name: Resolve the jnats version to test
        id: v
        env:
          INPUT_VERSION: ${{ github.event.inputs.version }}   # pass as data, don't interpolate into the script (injection-safe)
        run: |
          set -euo pipefail
          VERSION="${INPUT_VERSION:-}"
          if [ -z "$VERSION" ]; then
            # default: jnats' current main snapshot = its build.gradle jarVersion + "-SNAPSHOT"
            JARVER=$(curl -sf https://raw.githubusercontent.com/nats-io/nats.java/main/build.gradle \
                      | grep -oP 'def jarVersion\s*=\s*"\K[^"]+' | head -1)
            VERSION="${JARVER}-SNAPSHOT"
          fi
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "testing against jnats $VERSION"

      - name: Pin jnats ${{ steps.v.outputs.version }} for this run (not committed)
        run: sed -i "s#io.nats:jnats:[^'\"]*#io.nats:jnats:${{ steps.v.outputs.version }}#" build.gradle

      # --- build setup: mirror this repo's own build-pr.yml ---
      - uses: actions/setup-java@v5
        with:
          java-version: 25
          distribution: temurin
      - uses: gradle/actions/setup-gradle@v6
        with:
          gradle-version: current
      # (if the tests need a nats-server, install it here as build-pr.yml does)

      - name: Test against jnats ${{ steps.v.outputs.version }}
        run: ./gradlew --refresh-dependencies clean test
```

### Matrix

If your `build-pr.yml` tests across a matrix (e.g. several target-compatibility levels), the canary should too - a new jnats might break one target and not another. Resolve the version **once** in its own job so every matrix leg tests the same version (and you don't hit jnats main N times), then fan out. Use `fail-fast: false` so one failing target doesn't cancel the others. (This is what `java-active-passive` runs, over `tc: [8, 17, 21, 25]`.)

```yaml
name: jnats canary

on:
  schedule:
    - cron: "17 3 * * *"
  workflow_dispatch:
    inputs:
      version:
        description: "jnats version to test (blank = jnats' current main snapshot)"
        required: false

permissions:
  contents: read

jobs:
  resolve:
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.v.outputs.version }}
    steps:
      - name: Resolve the jnats version to test
        id: v
        env:
          INPUT_VERSION: ${{ github.event.inputs.version }}   # pass as data, don't interpolate into the script (injection-safe)
        run: |
          set -euo pipefail
          VERSION="${INPUT_VERSION:-}"
          if [ -z "$VERSION" ]; then
            JARVER=$(curl -sf https://raw.githubusercontent.com/nats-io/nats.java/main/build.gradle \
                      | grep -oP 'def jarVersion\s*=\s*"\K[^"]+' | head -1)
            VERSION="${JARVER}-SNAPSHOT"
          fi
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"

  test:
    needs: resolve
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        tc: [ 8, 17, 21, 25 ]      # match your build-pr.yml matrix
    env:
      TARGET_COMPATIBILITY: ${{ matrix.tc }}
      VERSION: ${{ needs.resolve.outputs.version }}
    steps:
      - uses: actions/checkout@v7
      - name: Pin jnats ${{ env.VERSION }} for this run (not committed)
        run: sed -i "s#io.nats:jnats:[^'\"]*#io.nats:jnats:${VERSION}#" build.gradle
      # --- build setup: mirror this repo's own build-pr.yml ---
      - uses: actions/setup-java@v5
        with:
          java-version: 25
          distribution: temurin
      - uses: gradle/actions/setup-gradle@v6
        with:
          gradle-version: current
      # (if the tests need a nats-server, install it here as build-pr.yml does)
      - name: Test against jnats ${{ env.VERSION }} (Java ${{ matrix.tc }})
        run: ./gradlew --refresh-dependencies clean test
```

## Notes

- **No token, no credentials.** It reads jnats main and resolves the snapshot over public HTTP, and uses only the automatic `GITHUB_TOKEN` for checkout.
- **Nothing is committed.** The `sed` edits the checked-out `build.gradle` in the runner and is discarded when the job ends; the consumer's pinned version is untouched.
- **`--refresh-dependencies` is required.** A `-SNAPSHOT` version string doesn't change when jnats republishes it - only the content does - so without a refresh Gradle may test a stale cached jar.
- **Frequency is a choice.** Once a day is enough for early warning and gives a steady daily green/red. If you want it tighter, shorten the cron; if you want to avoid re-testing an unchanged snapshot, gate the build on the snapshot's `maven-metadata.xml` `<lastUpdated>` stamp (e.g. keyed into an Actions cache) - but for most canaries the daily run is simpler and sufficient.
- **Run it on demand.** The `workflow_dispatch` trigger puts a "Run workflow" button in the Actions tab. Leave the `version` field blank to test jnats' current main snapshot, or type any version - a release like `2.27.0`, or a specific snapshot - to test that instead. Useful for vetting a jnats release before you adopt it, or reproducing a failure against a fixed version.

## Seeing results

- **Actions tab** - every run is green/red with logs; that's the history (a scheduled run has no PR, so it lives only here, not as a commit/PR check).
- **Failure email** - GitHub emails the workflow's last editor when a *scheduled* run fails (failures only, by default), so a broken canary reaches you without watching the tab. Tune it under Settings -> Notifications -> Actions.
- **README badge** - for at-a-glance status:
  ```markdown
  [![jnats canary](https://github.com/OWNER/REPO/actions/workflows/jnats-canary.yml/badge.svg)](https://github.com/OWNER/REPO/actions/workflows/jnats-canary.yml)
  ```
- Want it louder? Add an `if: failure()` step that opens an issue or pings Slack - that needs a token with the matching write scope.

`java-active-passive` is the reference implementation.

# Deploying

## Introduction

There are currently two pieces to the deployment, GitHub Actions + sonatype, with a few random extra bits of knowledge.

## Automated Deployments

As of `2.6.4` the release is automated using gradle nexus plugins. See the build.gradle file for details.

## Snapshot Release.
-SNAPSHOT releases are automatically made when anything is merged to main.

## Full Release
To do a full release, "Draft a new Release" from the [GitHub Releases](https://github.com/nats-io/nats.java/releases) page.

## After Full Release

Make a PR with these changes...

1. Bump the patch segment of the version in build.gradle.
   This ensures that on the next merge to the main branch, the published SNAPSHOT will have a new version.
   * If the release turns from a patch to a minor, you can make a small PR just to update this, which will also make the first snapshot.
   * You never need `-SNAPSHOT` on the version, the automated build deals with this.  
2. Update the readme where versions are noted.
3. Update the changelog.md
4. Make a PR with these changes, call it "Start <maj>.<min>.<patch>". 


# Deploying

## Introduction

There are currently two pieces to the deployment, travis + sonatype, with a few random extra bits of knowledge.

## Before you Release

1. Check that the Nats.java version is updated.
2. Check that the version in gradle.build is updated, including the jar file versions
3. Check dependency versions.
4. Check that the changelog.md is ready

## Automated release

As of 2.6.4 the release is automated using gradle nexus plugins. See the build.gradle file for details. To do a release, simply update values in "before you release" and tag a release on master.

The travis file uses two items to decide to deploy to sonatype/nexus:

* The branch is master
* There is a tag value

Pushing to master without a tag will result in a snapshot deployment, these can override each other.

The Java repository/build will have issues if you try to tag non-releases that have the same version number as an existing release. The gradle close and release process will fail if there is more than one repository staged. You may need to manually drop repositories from staging during testing on a single version number. These are in the sonatype UI under staging repositories and will start with io.nats.

If a release fails from travis, you can release manually, by clearing any staging repositories and following the steps below.

Also, keep in mind that repositories on sonatype are readonly so if you mess up, you may have to bump the release number just to try again.

## Manually Releasing

You can deploy manually by setting up your gradle.properties to have:

```ascii
signing.keyId=<TRAVIS_GPG_KEY_ID>
signing.password=<TRAVIS_KEY_PASSPHRASE>
signing.secretKeyRingFile=<PATH TO THE KEYRING>

ossrhUsername=<YOUR_JIRA_USER_NAME>
ossrhPassword=<YOUR_JIRA_PASSWORD>
```

the last line, if uncommented, will cause the deploy to go to your build folder, so that you can manually check it. Once you have the properties set up, simple type

```bash
> export TRAVIS_BRANCH=master
> export TRAVIS_TAG=<version>
> ./gradlew publishToSonatype
```

to upload to the repo. This will require you to go to sonatype to perform the actual close/release of your archive in the staging repositories tab. You can also perform the full release using:

```bash
> export TRAVIS_BRANCH=master
> export TRAVIS_TAG=<version>
> ./gradlew publishToSonatype closeAndReleaseRepository
```

## Manually Deploying a Snapshot

To deploy a snapshot you just want to have the branch and tag not set. So:

```bash
> ./gradlew publishToSonatype
```

You also don't need to close and release snapshots, and they can be written over.

## Automating the travis deploy and PGP

The global/secure values are used to deploy, they can be regenerated/created by someone with access to
the sonatype repository

You can use --add or copy and paste to the global section manually.
I had issues with the repo, so you may want to add -r nats-io/nats.java

```bash
> cd java-nats
> travis encrypt SONATYPE_USERNAME="<YOUR_JIRA_USER_NAME>" --add
> travis encrypt SONATYPE_PASSWORD="<YOUR_JIRA_PASSWORD>" --add
> travis encrypt GPG_KEY_ID="<TRAVIS_GPG_KEY_ID>" --add
> travis encrypt GPG_KEY_PASSPHRASE="<TRAVIS_KEY_PASSPHRASE>" --add
```

or use

```bash
> travis encrypt -i
```

to avoid passwords in shell history.

To get the signing key to travis, export it and put it in the .travis folder

```bash
> gpg --export-secret-key <TRAVIS_KEY_ID> > nats.travis.gpg
> travis encrypt-file .travis/nats.travis.gpg
```

Update the before install as instructed

NOTE - if your password has special characters to BASH - which may be required by sonatype, you need to escape them before you encrypt them. If you do not then bash will mess them up when Travis tries to set them.

## The signing key

The key is encrypted into github using the above instructions. The public key was uploaded to a central site by exporting it:

```bash
gpg --export -a "nats.io" > public.key
```

then manually uploading at `http://keys.gnupg.net:11371/`. So that sonatype can find it. You can also upload to `http://keyserver.ubuntu.com/` and `http://pool.sks-keyservers.net/pks/add`. You can also do this on the command line:

```bash
> gpg --keyserver keyserver.ubuntu.com --send-keys <keyid>
> gpg --keyserver pool.sks-keyservers.net --send-keys <keyid>
> gpg --keyserver keys.gnupg.net --send-keys <keyid>
> gpg --keyserver pgp.mit.edu --send-keys <keyid>
 ```

 The entire key process is very very painful, and seems to take time to propagate

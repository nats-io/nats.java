
# Deploying

## Introduction

There are currently two steps to the deployment, travis + sonatype, with a few random extra bits of knowledge.

Travis doesn't support sonatype deploy correctly. There is an issue where the various artifacts get split across multiple repositories. That code has been deleted from the travis file and manual releases are required.

~~Travis will either deploy a snapshot or a release based on the version in build.gradle. If you deploy a release build, you will need to manually go to sonatype to release it. Those builds are found in the staging area. (We need to try to automate this in the future.)~~

## Important note about release repositories

The gradle close and release process will fail if there is more than one repository staged. You may need to manually drop repositories from staging during testing on a single version number.

## Before you Release

1. Check that the Nats.java version is updated.
2. Check that the version in gradle.build is updated, including the jar file versions
3. Check dependency versions.
4. Check that the changelog.md is ready

## Manually Deploying

You can deploy manually by setting up your gradle.properties to have:

```ascii
signing.keyId=<TRAVIS_GPG_KEY_ID>
signing.password=<TRAVIS_KEY_PASSPHRASE>
signing.secretKeyRingFile=<PATH TO THE KEYRING>

ossrhUsername=<YOUR_JIRA_USER_NAME>
ossrhPassword=<YOUR_JIRA_PASSWORD>

#local_archives=true
```

the last line, if uncommented, will cause the deploy to go to your build folder, so that you can manually check it. Once you have the properties set up, simple type

```bash
> export TRAVIS_BRANCH=master
> ./gradlew uploadArchives
```

to upload to the repo.

## Automating the travis deploy

See [https://rishikeshdarandale.github.io/build/deploying-to-oss-sonatype-using-travis/](https://rishikeshdarandale.github.io/build/deploying-to-oss-sonatype-using-travis/) and [https://central.sonatype.org/pages/gradle.html](https://central.sonatype.org/pages/gradle.html)

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

## Releasing

### Using Gradle

I found the [gradle-nexus-staging-plugin](https://github.com/Codearte/gradle-nexus-staging-plugin/) which can release from staging.

From the readme, you can use the three tasks:

* closeRepository - closes an open repository with the uploaded artifacts. There should be just one open repository available in the staging profile (possible old/broken repositories can be dropped with Nexus GUI)
* releaseRepository - releases a closed repository (required to put artifacts to Maven Central aka The Central Repository)
* closeAndReleaseRepository - closes and releases a repository (an equivalent to closeRepository releaseRepository)

This last one will fail if the repo already exists using the specified version number

### Manual release

Once the uploadArchives completes, your artifact will be staged with Sonatype, but you need to verify it all looks good before it’s released from the staging area. To do this, you need to follow these steps:

1. Log into Sonatype Pro
2. Click “Staging Repositories” and scroll to the bottom of the list, look for ionats
3. Check the contents in the tab
4. Click “Close” at the top of the list
5. Click “Release”! at the top of the list

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
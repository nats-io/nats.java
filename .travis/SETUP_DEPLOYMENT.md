# Deploying NATS maven packages in Travis

This document describes the steps required to setup NATS' maven deployments in Travis.

## Key Generation

Generate your private and public keys:

```text
gpg --gen-key (The NATS Team, info@nats.io, <choose a gpg password>)
gpg --list-keys

---------------------------------------
pub   rsa2048 2018-04-05 [SC] [expires: 2020-04-04]
      24FC00CB8E7AA0F0AA73408AA6E4490E4A002F15
uid           [ultimate] The NATS Team <info@nats.io>
sub   rsa2048 2018-04-05 [E] [expires: 2020-04-04]
```

Add your key to a public keyserver:

```text
gpg --keyserver hkp://pool.sks-keyservers.net --recv-keys <your public key above>
```

Save the private key and gpg passphrase someplace safe.

## Create the encoded keyrings

Encode the keys to check in for travis.

```text
openssl aes-256-cbc -pass pass:<gpg password> -in ~/.gnupg/trustdb.gpg -out keyrings/secring.gpg.enc
openssl aes-256-cbc -pass pass:<gpg password> -in ~/.gnupg/pubring.kbx -out keyrings/pubring.gpg.enc
```

## Setup Travis

Login to Travis.ci with your Github credentials.

```text
travis login
```

Add the secure variables to Travis.  This adds "secret" tags in .Travis.yml.

```text
travis encrypt --add -r nats-io/java-nats ENCRYPTION_PASSWORD=<gpg encryption password>
travis encrypt --add -r nats-io/java-nats SONATYPE_USERNAME=<username>
travis encrypt --add -r nats-io/java-nats SONATYPE_PASSWORD=<password>
travis encrypt --add -r nats-io/java-nats GPG_KEYNAME==<gpg keyname (ex. 1C06698F)>
travis encrypt --add -r nats-io/java-nats GPG_PASSPHRASE=<gpg passphrase>
```

These are used throughout the pom for signing the deployment jars and for uploading to the sonatype repository.